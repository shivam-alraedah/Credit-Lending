from mal_pipeline.ingest.aecb_xml import parse_aecb_xml


SAMPLE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<AECBReport batchId="B-20250422-01" reportTs="2025-04-22T12:00:00Z">
  <Subject>
    <EmiratesId>784-1990-1234567-1</EmiratesId>
    <BureauScore>712</BureauScore>
    <Delinquencies>
      <Dpd30Plus>0</Dpd30Plus>
      <Dpd60Plus>0</Dpd60Plus>
    </Delinquencies>
  </Subject>
  <Subject>
    <EmiratesId>784-1985-7654321-2</EmiratesId>
    <BureauScore>640</BureauScore>
    <Delinquencies>
      <Dpd30Plus>1</Dpd30Plus>
    </Delinquencies>
  </Subject>
  <Subject>
    <BureauScore>500</BureauScore>
  </Subject>
</AECBReport>
"""


def test_parse_aecb_xml_extracts_subjects_and_skips_malformed():
    rows = parse_aecb_xml(SAMPLE_XML)
    assert len(rows) == 2
    r1, r2 = rows
    assert r1.emirates_id == "784-1990-1234567-1"
    assert r1.bureau_batch_id == "B-20250422-01"
    assert r1.bureau_score == 712
    assert r1.delinquency_flags == {"Dpd30Plus": "0", "Dpd60Plus": "0"}
    assert r2.bureau_score == 640
    assert r2.delinquency_flags == {"Dpd30Plus": "1"}
