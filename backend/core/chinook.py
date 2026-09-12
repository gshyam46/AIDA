"""Explicit mappings and independent SQL oracles for the public Chinook sample."""
from pathlib import Path
import hashlib

CHINOOK_SHA256 = "bdf635be69850bd3be09c9a2dbeef7ddfb80036bd3ef3381383cd03b61e4a61a"


def chinook_source():
    path = Path(__file__).resolve().parents[2] / "fixtures" / "chinook" / "Chinook_Sqlite.sqlite"
    if hashlib.sha256(path.read_bytes()).hexdigest() != CHINOOK_SHA256:
        raise ValueError("The pinned public Chinook sample checksum does not match.")
    manifest = {
        "version": 2, "name": "Chinook music store", "currency": "USD", "as_of": "2013-12-31",
        "date_from": "2009-01-01", "date_to": "2013-12-22",
        "fact": {"table": "InvoiceLine", "key": "InvoiceLineId", "columns": ["InvoiceLineId", "InvoiceId", "TrackId", "UnitPrice", "Quantity"]},
        "tables": {"invoices": {"table": "Invoice"}, "customers": {"table": "Customer"}, "tracks": {"table": "Track"}, "albums": {"table": "Album"}},
        "relations": [
            {"id": "line_invoice", "from": "fact", "from_column": "InvoiceId", "to": "invoices", "to_column": "InvoiceId", "kind": "many_to_one"},
            {"id": "invoice_customer", "from": "invoices", "from_column": "CustomerId", "to": "customers", "to_column": "CustomerId", "kind": "many_to_one"},
            {"id": "line_track", "from": "fact", "from_column": "TrackId", "to": "tracks", "to_column": "TrackId", "kind": "many_to_one"},
            {"id": "track_album", "from": "tracks", "from_column": "AlbumId", "to": "albums", "to_column": "AlbumId", "kind": "many_to_one"},
        ],
        "metrics": [
            {"id": "units_sold", "label": "Units sold", "description": "Sum of purchased quantities at invoice-line grain.", "aggregate": "SUM", "table": "fact", "column": "Quantity", "scale": 1, "format": "number"},
            {"id": "sale_price", "label": "Average sale price", "description": "Average actual unit price on invoice lines, in USD; this is not track catalog price or invoice total.", "aggregate": "AVG", "table": "fact", "column": "UnitPrice", "scale": 1, "format": "currency"},
            {"id": "invoice_count", "label": "Distinct invoices", "description": "Count of distinct invoice identifiers among the selected invoice lines.", "aggregate": "COUNT_DISTINCT", "table": "fact", "column": "InvoiceId", "scale": 1, "format": "number"},
            {"id": "line_count", "label": "Invoice line count", "description": "Number of purchased invoice lines.", "aggregate": "COUNT", "table": "fact", "scale": 1, "format": "number"},
        ],
        "dimensions": [
            {"id": "customer_country", "label": "Customer country", "table": "customers", "column": "Country", "type": "string"},
            {"id": "billing_country", "label": "Billing country", "table": "invoices", "column": "BillingCountry", "type": "string"},
            {"id": "album", "label": "Album", "table": "albums", "column": "Title", "type": "string"},
        ],
        "fields": [
            {"id": "catalog_price", "label": "Catalog price", "table": "tracks", "column": "UnitPrice", "type": "number"},
            {"id": "track_duration", "label": "Track duration", "table": "tracks", "column": "Milliseconds", "type": "number"},
        ],
        "date": {"table": "invoices", "column": "InvoiceDate"},
        "exists_relations": [{"id": "playlist_membership", "label": "Playlist membership", "table": "PlaylistTrack", "parent": "tracks", "parent_column": "TrackId", "child_column": "TrackId", "fields": []}],
        "examples": ["Units sold by customer country", "Average sale price by album", "Distinct invoices by billing country", "Units sold by month in 2013"],
    }
    return {"id": "chinook", "path": path, "manifest": manifest, "synthetic": True}


def chinook_cases():
    base = {"version": 2, "metrics": ["units_sold"], "dimensions": [], "filters": [], "having": [],
            "population": "primary", "set_operation": "union_all", "exists": None, "comparison": None,
            "date_from": None, "date_to": None, "sort": {"field": "units_sold", "direction": "desc"}, "limit": 100}
    def case(case_id, question, sql, **changes):
        return {"id": case_id, "source_id": "chinook", "question": question, "plan": {**base, **changes}, "oracle_sql": sql}
    return [
        case("chinook_country", "Units sold by customer country", 'SELECT c.Country AS customer_country, SUM(l.Quantity) AS units_sold FROM InvoiceLine l LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId LEFT JOIN Customer c ON i.CustomerId=c.CustomerId GROUP BY c.Country ORDER BY units_sold DESC, customer_country ASC LIMIT 100', dimensions=["customer_country"]),
        case("chinook_five_tables", "Units sold by album for customer country USA", 'SELECT a.Title AS album, SUM(l.Quantity) AS units_sold FROM InvoiceLine l LEFT JOIN Track t ON l.TrackId=t.TrackId LEFT JOIN Album a ON t.AlbumId=a.AlbumId LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId LEFT JOIN Customer c ON i.CustomerId=c.CustomerId WHERE c.Country=\'USA\' GROUP BY a.Title ORDER BY units_sold DESC, album ASC LIMIT 100', dimensions=["album"], filters=[{"field": "customer_country", "op": "eq", "value": "USA"}]),
        case("chinook_physical_price", "Average sale price by customer country where catalog price is greater than 1", 'SELECT c.Country AS customer_country, ROUND(AVG(l.UnitPrice),2) AS sale_price FROM InvoiceLine l LEFT JOIN Track t ON l.TrackId=t.TrackId LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId LEFT JOIN Customer c ON i.CustomerId=c.CustomerId WHERE t.UnitPrice>1 GROUP BY c.Country ORDER BY sale_price DESC, customer_country ASC LIMIT 100', metrics=["sale_price"], dimensions=["customer_country"], filters=[{"field": "catalog_price", "op": "gt", "value": 1}], sort={"field": "sale_price", "direction": "desc"}),
        case("chinook_having", "Units sold by billing country with units sold greater than 100", 'SELECT i.BillingCountry AS billing_country, SUM(l.Quantity) AS units_sold FROM InvoiceLine l LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId GROUP BY i.BillingCountry HAVING SUM(l.Quantity)>100 ORDER BY units_sold DESC, billing_country ASC LIMIT 100', dimensions=["billing_country"], having=[{"metric": "units_sold", "op": "gt", "value": 100}]),
        case("chinook_distinct", "Distinct invoices by customer country", 'SELECT c.Country AS customer_country, COUNT(DISTINCT l.InvoiceId) AS invoice_count FROM InvoiceLine l LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId LEFT JOIN Customer c ON i.CustomerId=c.CustomerId GROUP BY c.Country ORDER BY invoice_count DESC, customer_country ASC LIMIT 100', metrics=["invoice_count"], dimensions=["customer_country"], sort={"field": "invoice_count", "direction": "desc"}),
        case("chinook_month", "Units sold by month in 2013", 'SELECT substr(i.InvoiceDate,1,7) AS month, SUM(l.Quantity) AS units_sold FROM InvoiceLine l LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId WHERE substr(i.InvoiceDate,1,10)>=\'2013-01-01\' AND substr(i.InvoiceDate,1,10)<=\'2013-12-31\' GROUP BY substr(i.InvoiceDate,1,7) ORDER BY month ASC LIMIT 100', dimensions=["month"], date_from="2013-01-01", date_to="2013-12-31", sort={"field": "month", "direction": "asc"}),
        case("chinook_semijoin", "Units sold by customer country with playlist membership", 'SELECT c.Country AS customer_country, SUM(l.Quantity) AS units_sold FROM InvoiceLine l LEFT JOIN Invoice i ON l.InvoiceId=i.InvoiceId LEFT JOIN Customer c ON i.CustomerId=c.CustomerId LEFT JOIN Track t ON l.TrackId=t.TrackId WHERE EXISTS (SELECT 1 FROM PlaylistTrack p WHERE p.TrackId=t.TrackId) GROUP BY c.Country ORDER BY units_sold DESC, customer_country ASC LIMIT 100', dimensions=["customer_country"], exists={"relation": "playlist_membership", "negate": False, "filters": []}),
    ]
