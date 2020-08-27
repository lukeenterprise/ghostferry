package ghostferry

import (
	"math"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

// StateTracker design
// ===================
//
// General Overview
// ----------------
//
// The state tracker keeps track of the progress of Ghostferry so it can be
// interrupted and resumed. The state tracker is supposed to be initialized and
// managed by the Ferry. Each Ghostferry components, such as the `BatchWriter`,
// will get passed an instance of the StateTracker. During the run, the
// components will update their last successful components to the state tracker
// instance given via the state tracker API defined here.
//
// The states stored in the state tracker can be copied into a
// serialization-friendly struct (`SerializableState`), which can then be
// dumped using something like JSON. Assuming the rest of Ghostferry used the
// API of the state tracker correctlym this can be done at any point during the
// Ghostferry run and the resulting state can be resumed from without data
// loss.  The same `SerializableState` is used as an input to `Ferry`, which
// will instruct the `Ferry` to resume a previously interrupted run.

type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	CompletedTables map[string]bool
	BatchProgress   map[string]map[uint64]*BatchProgress

	LastWrittenBinlogPosition                 mysql.Position
	BinlogVerifyStore                         BinlogVerifySerializedStore
	LastStoredBinlogPositionForInlineVerifier mysql.Position
	LastStoredBinlogPositionForTargetVerifier mysql.Position
}

func (s *SerializableState) MinSourceBinlogPosition() mysql.Position {
	nilPosition := mysql.Position{}
	if s.LastWrittenBinlogPosition == nilPosition {
		return s.LastStoredBinlogPositionForInlineVerifier
	}

	if s.LastStoredBinlogPositionForInlineVerifier == nilPosition {
		return s.LastWrittenBinlogPosition
	}

	if s.LastWrittenBinlogPosition.Compare(s.LastStoredBinlogPositionForInlineVerifier) >= 0 {
		return s.LastStoredBinlogPositionForInlineVerifier
	} else {
		return s.LastWrittenBinlogPosition
	}
}

func (s *SerializableState) CalculateTableProgress(table string) uint64 {
	var totalPercentage uint64 = 0

	batches, ok := s.BatchProgress[table]
	if !ok {
		return 0
	}

	for _, batch := range batches {
		totalPercentage += batch.completedPercentage
	}

	return totalPercentage / uint64(len(batches))
}

type StateTracker struct {
	BinlogRWMutex *sync.RWMutex
	CopyRWMutex   *sync.RWMutex

	lastWrittenBinlogPosition                 mysql.Position
	lastStoredBinlogPositionForInlineVerifier mysql.Position
	lastStoredBinlogPositionForTargetVerifier mysql.Position

	completedTables map[string]bool
	batchProgress   map[string]map[uint64]*BatchProgress

	logger *logrus.Entry
}

type BatchProgress struct {
	StartPaginationKey  uint64
	EndPaginationKey    uint64
	LatestPaginationKey uint64
	Completed           bool
	completedPercentage uint64
}

func NewStateTracker() *StateTracker {
	return &StateTracker{
		BinlogRWMutex: &sync.RWMutex{},
		CopyRWMutex:   &sync.RWMutex{},

		completedTables: make(map[string]bool),
		batchProgress:   make(map[string]map[uint64]*BatchProgress),

		logger: logrus.WithField("tag", "state_tracker"),
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTrackerFromSerializedState(serializedState *SerializableState) *StateTracker {
	s := NewStateTracker()
	s.completedTables = serializedState.CompletedTables
	s.batchProgress = serializedState.BatchProgress
	s.lastWrittenBinlogPosition = serializedState.LastWrittenBinlogPosition
	s.lastStoredBinlogPositionForInlineVerifier = serializedState.LastStoredBinlogPositionForInlineVerifier
	s.lastStoredBinlogPositionForTargetVerifier = serializedState.LastStoredBinlogPositionForInlineVerifier
	return s
}

func (s *StateTracker) UpdateLastResumableSourceBinlogPosition(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastWrittenBinlogPosition = pos
}

func (s *StateTracker) UpdateLastResumableSourceBinlogPositionForInlineVerifier(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastStoredBinlogPositionForInlineVerifier = pos
}

func (s *StateTracker) UpdateLastResumableBinlogPositionForTargetVerifier(pos mysql.Position) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.lastStoredBinlogPositionForTargetVerifier = pos
}

func (s *StateTracker) RegisterBatch(table string, index uint64, startPaginationKey uint64, endPaginationKey uint64) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	if _, ok := s.batchProgress[table]; !ok {
		s.batchProgress[table] = make(map[uint64]*BatchProgress)
	}

	s.batchProgress[table][index] = &BatchProgress{
		StartPaginationKey:  startPaginationKey,
		EndPaginationKey:    endPaginationKey,
		LatestPaginationKey: startPaginationKey,
		Completed:           false,
	}
}

func (s *StateTracker) UpdateBatchPosition(table string, index uint64, latestPaginationKey uint64) {
	s.CopyRWMutex.Lock()

	logger := s.logger.WithField("table", table).WithField("batchIndex", index)

	_, tableFound := s.batchProgress[table]
	if !tableFound {
		logger.Error("tried to mark non-existing batch as completed")
		return
	}

	batch, batchFound := s.batchProgress[table][index]
	if !batchFound {
		logger.Error("tried to mark non-existing batch as completed")
		return
	}

	// Set latest key to EndPaginationKey if we are ahead due to a fragmented PK
	batch.LatestPaginationKey = uint64(math.Min(float64(latestPaginationKey), float64(batch.EndPaginationKey)))

	if batch.EndPaginationKey == batch.LatestPaginationKey {
		logger.Debug("marking batch as completed")
		batch.Completed = true
		batch.completedPercentage = 100
		s.CopyRWMutex.Unlock()

		if s.areAllBatchesCompleted(table) {
			logger.Info("marking table as done")
			s.MarkTableAsCompleted(table)
		}
	} else {
		batch.completedPercentage = 100 * (batch.LatestPaginationKey - batch.StartPaginationKey) / (batch.EndPaginationKey - batch.StartPaginationKey)
		s.CopyRWMutex.Unlock()
	}
}

func (s *StateTracker) IsBatchComplete(table string, index uint64) bool {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	if _, ok := s.batchProgress[table]; !ok {
		return false
	}

	batch, ok := s.batchProgress[table][index]
	if !ok {
		return false
	}

	return batch.Completed
}

func (s *StateTracker) areAllBatchesCompleted(table string) bool {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	if _, ok := s.batchProgress[table]; !ok {
		s.logger.WithField("table", table).Error("tried to get batch status for non-existing table")
		return false
	}

	for _, progress := range s.batchProgress[table] {
		if !progress.Completed {
			return false
		}
	}

	return true
}

func (s *StateTracker) MarkTableAsCompleted(table string) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	s.completedTables[table] = true
}

func (s *StateTracker) IsTableComplete(table string) bool {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	return s.completedTables[table]
}

func (s *StateTracker) Serialize(lastKnownTableSchemaCache TableSchemaCache, binlogVerifyStore *BinlogVerifyStore) *SerializableState {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	state := &SerializableState{
		GhostferryVersion:                         VersionString,
		LastKnownTableSchemaCache:                 lastKnownTableSchemaCache,
		CompletedTables:                           make(map[string]bool),
		BatchProgress:                             make(map[string]map[uint64]*BatchProgress),
		LastWrittenBinlogPosition:                 s.lastWrittenBinlogPosition,
		LastStoredBinlogPositionForInlineVerifier: s.lastStoredBinlogPositionForInlineVerifier,
		LastStoredBinlogPositionForTargetVerifier: s.lastStoredBinlogPositionForTargetVerifier,
	}

	if binlogVerifyStore != nil {
		state.BinlogVerifyStore = binlogVerifyStore.Serialize()
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}

	for batchIndex, progress := range s.batchProgress {
		state.BatchProgress[batchIndex] = progress
	}

	return state
}
