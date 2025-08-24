## Oracle PL/SQL package for export data to CSV/XLS/JSON file

A PL/SQL package for exporting query results to files (written to an Oracle DIRECTORY) or to a BLOB variable in format CSV, XLS (XML Table 2003) or JSON.
Output encoding is UTF-8 (optionally with BOM). Full support for national character sets.



## Features

Export to file (via Oracle DIRECTORY) or BLOB.

Supports all standard Oracle datatypes (except LONG RAW and BFILE).

Proper CSV formatting: quoted fields, semicolon delimiter by default.

National character set support.

Optional compression (gz).

Flexible control of headers, BOM.

For formatted DATE, NUMBER, TIMESTAMP types in CSV use ALTER SESSION SET NLS_...

If exceeded Excel limits - size of cells or count of rows - raise exception.

Binary data export as hex strings



## Examples

```SQL
begin 
  export.export2csv('select * from all_tables', 'EXPORT_DIR', 'all_tables.csv'); 
end;

begin 
  export.export2xls('select * from all_tables', 'EXPORT_DIR', 'all_tables.xls'); 
end;

begin 
  export.export2json('select * from all_tables', 'EXPORT_DIR', 'all_tables.json', p_compress => true); 
end;

declare
  c sys_refcursor; 
begin
  open c for select * from all_tables; 
  export.export2xls(c, 'EXPORT_DIR', 'all_tables.xls', p_compress => true);
end;

declare
  p_outblob blob;
begin 
  export.export2csv('select * from all_tables', p_outblob, p_header => false, p_bom => false, p_compress => false); 
end;
```



## Install

```SQL
--sql
@EXPORT.pck
```



## Requirements

Oracle Database 12c+

