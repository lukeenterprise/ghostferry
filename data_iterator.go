package ghostferry

import (
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync"

	"github.com/sirupsen/logrus"
)

type PaginationKey struct {
	table              *TableSchema
	batchIndex         uint64
	startPaginationKey uint64
	endPaginationKey   uint64
}

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	paginationKeys map[string]*PaginationKey
	batchListeners []func(*RowBatch) error
	doneListeners  []func() error
	logger         *logrus.Entry
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.logger = logrus.WithField("tag", "data_iterator")

	// If a state tracker is not provided, then the caller doesn't care about
	// tracking state. However, some methods are still useful so we initialize
	// a minimal local instance.
	if d.StateTracker == nil {
		d.StateTracker = NewStateTracker()
	}

	d.logger.WithField("tablesCount", len(tables)).Info("starting data iterator run")
	tablesWithData, emptyTables, err := MinMaxPaginationKeys(d.DB, tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	for _, table := range emptyTables {
		d.StateTracker.MarkTableAsCompleted(table.String())
	}

	d.paginationKeys = make(map[string]*PaginationKey, len(tablesWithData))

	for table, keys := range tablesWithData {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			d.logger.WithField("table", tableName).Warn("table already completed, skipping")
			continue
		}
		d.paginationKeys[tableName] = &PaginationKey{
			table:              table,
			startPaginationKey: keys.MinPaginationKey,
			endPaginationKey:   keys.MaxPaginationKey,
		}
	}

	batchQueue := make(chan *PaginationKey)
	wg := &sync.WaitGroup{}
	wg.Add(d.Concurrency)

	for i := 0; i < d.Concurrency; i++ {
		go func() {
			defer wg.Done()

			for {
				PaginationKey, ok := <-batchQueue
				if !ok {
					break
				}

				table := PaginationKey.table
				tableName := table.String()

				batchLogger := d.logger.WithFields(logrus.Fields{
					"table":              tableName,
					"startPaginationKey": PaginationKey.startPaginationKey,
					"endPaginationKey":   PaginationKey.endPaginationKey,
					"batchIndex":         PaginationKey.batchIndex,
				})

				batchLogger.Debug("setting up new batch cursor")
				cursor := d.CursorConfig.NewCursor(table, PaginationKey.startPaginationKey, PaginationKey.endPaginationKey)
				if d.SelectFingerprint {
					if len(cursor.ColumnsToSelect) == 0 {
						cursor.ColumnsToSelect = []string{"*"}
					}

					cursor.ColumnsToSelect = append(cursor.ColumnsToSelect, table.RowMd5Query())
				}

				err := cursor.Each(func(batch *RowBatch) error {
					batchValues := batch.Values()
					paginationKeyIndex := batch.PaginationKeyIndex()

					batch.batchIndex = PaginationKey.batchIndex

					batchLogger.WithField("size", batch.Size()).Debug("row event")
					metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
						MetricTag{"table", table.Name},
						MetricTag{"source", "table"},
					}, 1.0)

					if d.SelectFingerprint {
						fingerprints := make(map[uint64][]byte)
						rows := make([]RowData, batch.Size())

						for i, rowData := range batchValues {
							paginationKey, err := rowData.GetUint64(paginationKeyIndex)
							if err != nil {
								batchLogger.WithError(err).Error("failed to get paginationKey data")
								return err
							}

							fingerprints[paginationKey] = rowData[len(rowData)-1].([]byte)
							rows[i] = rowData[:len(rowData)-1]
						}

						batch.values = rows
						batch.fingerprints = fingerprints
					}

					for _, listener := range d.batchListeners {
						err := listener(batch)
						if err != nil {
							batchLogger.WithError(err).Error("failed to process row batch with listeners")
							return err
						}
					}

					return nil
				})

				if err != nil {
					switch e := err.(type) {
					case BatchWriterVerificationFailed:
						d.logger.WithField("incorrect_tables", e.table).Error(e.Error())
						d.ErrorHandler.Fatal("inline_verifier", err)
					default:
						d.logger.WithError(err).Error("failed to iterate table")
						d.ErrorHandler.Fatal("data_iterator", err)
					}

				}
			}
		}()
	}

	for _, key := range d.paginationKeys {
		tableName := key.table.String()
		stateBatches, loadBatchesFromState := d.StateTracker.batchProgress[tableName]

		if loadBatchesFromState {
			for batchIndex, batch := range stateBatches {
				if batch.Completed {
					continue
				}

				batchQueue <- &PaginationKey{
					table:              key.table,
					startPaginationKey: batch.LatestPaginationKey,
					endPaginationKey:   batch.EndPaginationKey,
					batchIndex:         batchIndex,
				}
			}
		} else {
			// Set start to minus one since cursor is searching for greater values
			tableStartPaginationKey := key.startPaginationKey - 1
			tableEndPaginationKey := key.endPaginationKey

			// Number of batches are set to number of processes, unless each batch becomes smaller than the cursor size
			keyInterval := tableEndPaginationKey - tableStartPaginationKey
			batchSize := Max(keyInterval/uint64(d.Concurrency), d.CursorConfig.BatchSize)

			d.logger.WithFields(logrus.Fields{
				"table":              tableName,
				"batchSize":          batchSize,
				"endPaginationKey":   tableEndPaginationKey,
				"startPaginationKey": tableStartPaginationKey,
			}).Debugf("queueing %d batches", (keyInterval/batchSize)+1)

			for batchStartPaginationKey := tableStartPaginationKey; batchStartPaginationKey < tableEndPaginationKey; batchStartPaginationKey += batchSize {
				batchEndPaginationKey := batchStartPaginationKey + batchSize
				batchIndex := (batchStartPaginationKey - tableStartPaginationKey) / batchSize

				// Set batchEndPaginationKey to endPaginationKey if out of bounds
				if batchEndPaginationKey > tableEndPaginationKey {
					batchEndPaginationKey = tableEndPaginationKey
				}

				paginationKey := &PaginationKey{
					table:              key.table,
					startPaginationKey: batchStartPaginationKey,
					endPaginationKey:   batchEndPaginationKey,
					batchIndex:         batchIndex,
				}

				d.StateTracker.RegisterBatch(tableName, paginationKey.batchIndex, paginationKey.startPaginationKey, paginationKey.endPaginationKey)
				batchQueue <- paginationKey

				// Protect against uint64 overflow, this might happen if the table is full
				if batchStartPaginationKey >= (^uint64(0) - batchSize) {
					batchSize = ^uint64(0) - batchStartPaginationKey
					if batchSize == 0 {
						break
					}
				}
			}
		}
	}

	d.logger.Info("done queueing tables to be iterated")
	close(batchQueue)

	wg.Wait()
	for _, listener := range d.doneListeners {
		listener()
	}
}

func (d *DataIterator) AddBatchListener(listener func(*RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}
