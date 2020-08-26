require "test_helper"

class CallbacksTest < GhostferryTestCase
  def test_progress_callback
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    progress = []
    ghostferry.on_callback("progress") do |progress_data|
      progress << progress_data
    end

    ghostferry.run

    assert progress.length >= 1

    assert_equal "done", progress.last["CurrentState"]

    assert_equal 5000, progress.last["Tables"]["gftest.test_table_1"]["TargetPaginationKey"]
    assert_equal "completed", progress.last["Tables"]["gftest.test_table_1"]["CurrentAction"]

    refute progress.last["LastSuccessfulBinlogPos"]["Name"].nil?
    refute progress.last["LastSuccessfulBinlogPos"]["Pos"].nil?
    assert progress.last["BinlogStreamerLag"] > 0
    assert_equal progress.last["LastSuccessfulBinlogPos"], progress.last["FinalBinlogPos"]

    assert_equal false, progress.last["Throttled"]

    assert_equal "Inline", progress.last["VerifierType"]
    assert progress.last["VerifierMessage"].start_with?("BinlogVerifyStore.currentRowCount =")

    assert progress.last["TimeTaken"] > 0
  end
end
