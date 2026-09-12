# Chinook v1.4.5

Unmodified public SQLite sample from [Chinook's official release](https://github.com/lerocha/chinook-database/releases/tag/v1.4.5), downloaded September 12, 2026.

- File: `Chinook_Sqlite.sqlite`
- SHA-256: `bdf635be69850bd3be09c9a2dbeef7ddfb80036bd3ef3381383cd03b61e4a61a`
- License: MIT, copyright Luis Rocha; the full notice is in [LICENSE.md](LICENSE.md).
- 11 related tables. AIDA's approved mapping uses invoice lines, invoices, customers, tracks, albums and playlist membership. It excludes personal names, addresses, contact information and raw records.

The database is retained unchanged so evaluation exercises a schema and data produced independently of AIDA. Business mappings and independent oracle queries are in `backend/core/chinook.py`. Units sold sums invoice-line Quantity; average sale price averages invoice-line UnitPrice. Invoice Total and track catalog UnitPrice are deliberately distinct columns and must not substitute for those metrics.
