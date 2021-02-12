select
sourcefilename,
ocr_metadata -> 'search_quality' ->> 'expected_fields' as expected_fields,
ocr_metadata -> 'search_quality' ->> 'found_fields' as found_fields,
ocr_metadata -> 'search_quality' ->> 'found_percentage' as found_percentage,
ocr_metadata -> 'read_quality' ->> 'count_less_than_80_percent' as qty_read_less_than_80_pct,
ocr_metadata -> 'read_quality' ->> 'high_quality_read_percent' as read_quality
from flint_claimant