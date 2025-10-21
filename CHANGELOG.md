# Changelog

## 2025-10-14

### VIN Extraction Improvements

- **Enhanced VIN detection with sliding window**: When OCR text chunks are less than 25 characters, the extractor now tries shifting left and right by up to 8 positions to capture VINs that have logo text or other characters adjacent to them
- **VIN suffix rule pattern**: Added pattern matching for 11 alphanumeric + 6 digits (last 6 chars of VIN are always numbers) to improve detection accuracy
- **Improved autocorrection tracking**: Notes field now shows all found VINs with their corrections in format `ORIGINALVIN → CORRECTEDVIN`, or just `VIN` if no correction was needed
- **Completed claude_data mode**: Full implementation of document-based VIN extraction using Claude API with document type instead of image type
- **Code cleanup**: Removed nested helper functions in `_process_vin`, inlined B/8 swap logic, and used sets for better deduplication in `_extract_candidates`

### Bug Fixes

- **Fixed duplicate correction notes**: When multiple raw VIN candidates (e.g., from sliding window) corrected to the same valid VIN, the notes field would show multiple correction entries for a single VIN. Now properly deduplicates corrections to show only one entry per unique corrected VIN
- **Removed redundant code**: Eliminated useless if/else branches in sliding window that performed identical actions, and removed duplicate Pattern 4 search on cleaned_text (already covered by Pattern 3)
- **Code refactoring**: Extracted duplicate VIN processing loops into shared `_process_vin_candidates()` helper method, reducing code duplication across all three extraction modes (google_vision, claude_image, claude_data)
- **Added comprehensive documentation**: Added detailed docstrings and inline comments throughout VIN extraction code for better maintainability

Files modified: `components/utils/vins.py`
