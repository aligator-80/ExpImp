## Oracle PL/SQL package for export data to CSV file

A PL/SQL package for exporting query results to CSV files (written to an Oracle DIRECTORY) or to a BLOB variable.
Output encoding is UTF-8 (optionally with BOM). Full support for national character sets.



## Features

Export to file (via Oracle DIRECTORY) or BLOB.

Supports all standard Oracle datatypes (except LONG RAW and BFILE).

Proper CSV formatting: quoted fields, semicolon delimiter by default.

National character set support.

Optional compression (gz).

Flexible control of headers, BOM.

For formatted DATE, NUMBER, TIMESTAMP types use ALTER SESSION SET NLS_...



## Examples

```SQL
begin 
  expimp.export2csv('select * from all_tables', 'EXPORT_DIR', 'all_tables.csv'); 
end;

declare
  c sys_refcursor; 
begin
  open c for select * from all_tables; 
  expimp.export2csv(c, 'EXPORT_DIR', 'all_tables.csv', p_compress => true);
end;

declare
  p_outblob blob;
begin 
  expimp.export2csv('select * from all_tables', p_outblob, p_header => false, p_bom => false, p_compress => false); 
end;
```



## Install

```SQL
--sql
@EXPIMP.pck
```



## Requirements

Oracle Database 12c+

