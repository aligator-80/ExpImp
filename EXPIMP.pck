create or replace package sha3.EXPIMP is

  ---------------- Examples set format for export2csv -------------------
  --ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI:SS';
  --ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '. ';
  --ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF3';  
  --ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'DD.MM.YYYY HH24:MI:SS.FF3 TZH:TZM';
 
  ---------------------------- Parameters -------------------------------
  -- p_query     - text of SQL query
  -- p_refcursor - cursor of SQL query
  -- p_dir       - directory of output file
  -- p_file      - name of output file
  -- p_outlob    - blob as output
  -- p_header    - include header in output?
  -- p_bom       - include BOM (UTF8) in output?
  -- p_compress  - compress output (it append .gz to filename)?
  
  procedure export2csv(p_query    clob,
                       p_dir      varchar2,
                       p_file     varchar2,
                       p_header   boolean default true,
                       p_bom      boolean default true,
                       p_compress boolean default false);
                       
  procedure export2csv(p_refcursor in out sys_refcursor,
                       p_dir       varchar2,
                       p_file      varchar2,
                       p_header    boolean default true,
                       p_bom       boolean default true,
                       p_compress  boolean default false);

  procedure export2csv(p_query    clob,
                       p_outlob   out blob, 
                       p_header   boolean default true,
                       p_bom      boolean default true,
                       p_compress boolean default false);
                       
  procedure export2csv(p_refcursor in out sys_refcursor,
                       p_outlob    out blob, 
                       p_header    boolean default true,
                       p_bom       boolean default true,
                       p_compress  boolean default false);

end EXPIMP;
/
create or replace package body sha3.EXPIMP is 

  --------------------- PRIVATE -------------------------
 
  procedure export2csv(p_cursor   in out integer,
                       p_dir      varchar2,
                       p_file     varchar2,
                       p_header   boolean,
                       p_bom      boolean,
                       p_compress boolean) 
  as    
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab; 
    p_string    varchar2(32767);  
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob; 
    p_tmpblob   blob;
    p_cnt       number;   
    i           pls_integer := 0;
    j           pls_integer := 0;
    p_length    pls_integer;
    p_quote     raw(1) := utl_i18n.string_to_raw('"', 'AL32UTF8'); 
    p_semicolon raw(1) := utl_i18n.string_to_raw(';', 'AL32UTF8'); 
    p_newline   raw(2) := utl_i18n.string_to_raw(chr(13) || chr(10), 'AL32UTF8');    
    p_file_ptr  utl_file.file_type := utl_file.fopen(p_dir, p_file, 'wb'); 
    p_bfile     bfile := bfilename(p_dir, p_file);
    
    procedure write_clob(p_data in out nocopy clob character set any_cs)
    as 
      p_dest_offset  pls_integer;
      p_src_offset   pls_integer;
      p_warning      pls_integer; 
      p_lang_context pls_integer := dbms_lob.default_lang_ctx;
    begin 
      p_data := '"' || replace(p_data, '"', '""') || '"'; 
          
      if (i < p_col_count) then
        p_data := p_data || ';';  
      end if; 
            
      p_dest_offset := 1;
      p_src_offset := 1;
      dbms_lob.trim(p_tmpblob, 0);
          
      dbms_lob.convertToBlob(dest_lob     => p_tmpblob,
                             src_clob     => p_data,
                             amount       => dbms_lob.lobmaxsize,
                             dest_offset  => p_dest_offset,
                             src_offset   => p_src_offset,
                             blob_csid    => nls_charset_id('AL32UTF8'),
                             lang_context => p_lang_context,
                             warning      => p_warning);
            
      j := 0;
      p_length := dbms_lob.getlength(p_tmpblob);
          
      while (j < p_length) loop 
        utl_file.put_raw(p_file_ptr, dbms_lob.substr(p_tmpblob, least(p_length - j, 32767), j + 1));
        j := j + 32767;
      end loop; 
    end write_clob;
  begin   
    dbms_lob.createtemporary(p_tmpblob, true);   
    
    if p_bom then
      utl_file.put_raw(p_file_ptr, 'EFBBBF');
    end if; 
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if;
      
      if p_header then
        if (i < p_col_count) then
          utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('"' || replace(p_desc_tab(i).col_name, '"', '""') || '";', 'AL32UTF8'));
        else
          utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('"' || replace(p_desc_tab(i).col_name, '"', '""') || '"', 'AL32UTF8'));
        end if; 
      end if; 
    end loop;
     
    if p_header then
      utl_file.put_raw(p_file_ptr, p_newline);
    end if;  
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
    
      for i in 1..p_col_count loop 
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then
            dbms_sql.column_value(p_cursor, i, p_nclob); 
            PRAGMA INLINE (write_clob, 'YES');
            write_clob(p_nclob); 
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob); 
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_clob); 
            elsif (p_desc_tab(i).col_type = 8) then  
              p_cnt := 0;
              dbms_sql.column_value_long(p_cursor, i, 16000, p_cnt, p_string, p_length);  
              utl_file.put_raw(p_file_ptr, p_quote); 
            
              while (p_length > 0) loop  
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(replace(p_string, '"', '""'), 'AL32UTF8'));  
                
                p_cnt := p_cnt + p_length;
                dbms_sql.column_value_long(p_cursor, i, 16000, p_cnt, p_string, p_length); 
              end loop; 
              
              utl_file.put_raw(p_file_ptr, p_quote); 
              
              if (i < p_col_count) then
                utl_file.put_raw(p_file_ptr, p_semicolon);   
              end if;  
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);  
              utl_file.put_raw(p_file_ptr, p_quote); 
              
              j := 0;
              p_length := dbms_lob.getlength(p_blob);  
              
              while (j < p_length) loop 
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(rawtohex(dbms_lob.substr(p_blob, least(p_length - j, 32767), j + 1)), 'AL32UTF8')); 
                j := j + 32767;
              end loop; 
              
              utl_file.put_raw(p_file_ptr, p_quote); 
            
              if (i < p_col_count) then
                utl_file.put_raw(p_file_ptr, p_semicolon);  
              end if;    
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              p_length := length(p_string);    
              
              if (p_length > 16000) then 
                dbms_lob.trim(p_clob, 0);
                dbms_lob.writeappend(p_clob, 1, p_string); 
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_clob); 
              else 
                p_string := '"' || replace(p_string, '"', '""') || '"';
              
                if (i < p_col_count) then
                  p_string := p_string || ';';  
                end if; 
                 
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_string, 'AL32UTF8')); 
              end if;
            end if;  
          end if; 
        else
          p_string := '"<Unsupported type>"';
              
          if (i < p_col_count) then
            p_string := p_string || ';';  
          end if; 
                 
          utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_string, 'AL32UTF8')); 
        end if;  
      end loop;  
          
      utl_file.put_raw(p_file_ptr, p_newline); 
    end loop; 
     
    dbms_sql.close_cursor(p_cursor);  
    utl_file.fclose(p_file_ptr);    
    
    if p_compress then 
      dbms_lob.open(p_bfile); 
      dbms_lob.loadfromfile(p_tmpblob, p_bfile, dbms_lob.lobmaxsize);  
      p_tmpblob := utl_compress.lz_compress(p_tmpblob); 
      dbms_lob.close(p_bfile);  
      
      p_file_ptr := utl_file.fopen(p_dir, p_file, 'wb');
      
      j := 0;
      p_length := dbms_lob.getlength(p_tmpblob);
            
      while (j < p_length) loop 
        utl_file.put_raw(p_file_ptr, dbms_lob.substr(p_tmpblob, least(p_length - j, 32767), j + 1));
        j := j + 32767;
      end loop; 
        
      utl_file.fclose(p_file_ptr);   
    end if; 
    
    dbms_lob.freetemporary(p_tmpblob); 
  end export2csv; 

  procedure export2csv(p_cursor   in out integer,
                       p_outlob   in out blob, 
                       p_header   boolean default true,
                       p_bom      boolean default true,
                       p_compress boolean) 
  as    
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab; 
    p_raw       raw(32767);
    p_string    varchar2(32767);  
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob; 
    p_tmpblob   blob;
    p_cnt       number;   
    i           pls_integer := 0; 
    p_length    pls_integer;
    p_quote     raw(1) := utl_i18n.string_to_raw('"', 'AL32UTF8'); 
    p_semicolon raw(1) := utl_i18n.string_to_raw(';', 'AL32UTF8');   
    p_newline   raw(2) := utl_i18n.string_to_raw(chr(13) || chr(10), 'AL32UTF8');   
    
    procedure write_clob(p_data in out nocopy clob character set any_cs)
    as 
      p_dest_offset  pls_integer;
      p_src_offset   pls_integer;
      p_warning      pls_integer; 
      p_lang_context pls_integer := dbms_lob.default_lang_ctx;
    begin 
      p_data := '"' || replace(p_data, '"', '""') || '"'; 
          
      if (i < p_col_count) then
        p_data := p_data || ';';  
      end if; 
            
      p_dest_offset := 1;
      p_src_offset := 1;
      dbms_lob.trim(p_tmpblob, 0);
          
      dbms_lob.convertToBlob(dest_lob     => p_tmpblob,
                             src_clob     => p_data,
                             amount       => dbms_lob.lobmaxsize,
                             dest_offset  => p_dest_offset,
                             src_offset   => p_src_offset,
                             blob_csid    => nls_charset_id('AL32UTF8'),
                             lang_context => p_lang_context,
                             warning      => p_warning);
       
      dbms_lob.append(p_outlob, p_tmpblob);  
    end write_clob;
  begin   
    dbms_lob.createtemporary(p_tmpblob, true);   
    
    if p_bom then 
      dbms_lob.writeappend(p_outlob, 3, 'EFBBBF'); 
    end if; 
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if;
      
      if p_header then
        if (i < p_col_count) then
          p_raw := utl_i18n.string_to_raw('"' || replace(p_desc_tab(i).col_name, '"', '""') || '";', 'AL32UTF8');
          dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
        else
          p_raw := utl_i18n.string_to_raw('"' || replace(p_desc_tab(i).col_name, '"', '""') || '"', 'AL32UTF8');
          dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
        end if; 
      end if; 
    end loop;
     
    if p_header then 
      dbms_lob.writeappend(p_outlob, 2, p_newline);  
    end if;  
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
    
      for i in 1..p_col_count loop 
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then
            dbms_sql.column_value(p_cursor, i, p_nclob); 
            PRAGMA INLINE (write_clob, 'YES');
            write_clob(p_nclob); 
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob); 
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_clob); 
            elsif (p_desc_tab(i).col_type = 8) then  
              p_cnt := 0;
              dbms_sql.column_value_long(p_cursor, i, 16000, p_cnt, p_string, p_length);   
              dbms_lob.writeappend(p_outlob, 1, p_quote);   
            
              while (p_length > 0) loop   
                p_raw :=  utl_i18n.string_to_raw(replace(p_string, '"', '""'), 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
                
                p_cnt := p_cnt + p_length;
                dbms_sql.column_value_long(p_cursor, i, 16000, p_cnt, p_string, p_length); 
              end loop; 
              
              dbms_lob.writeappend(p_outlob, 1, p_quote);   
              
              if (i < p_col_count) then 
                dbms_lob.writeappend(p_outlob, 1, p_semicolon);   
              end if;  
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);   
              dbms_lob.writeappend(p_outlob, 1, p_quote);
              dbms_lob.append(p_outlob, p_blob); 
              dbms_lob.writeappend(p_outlob, 1, p_quote);
            
              if (i < p_col_count) then 
                dbms_lob.writeappend(p_outlob, 1, p_semicolon);
              end if;    
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              p_length := length(p_string);    
              
              if (p_length > 16000) then 
                dbms_lob.trim(p_clob, 0);
                dbms_lob.writeappend(p_clob, 1, p_string); 
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_clob); 
              else 
                p_string := '"' || replace(p_string, '"', '""') || '"';
              
                if (i < p_col_count) then
                  p_string := p_string || ';';  
                end if; 
                 
                p_raw := utl_i18n.string_to_raw(p_string, 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
              end if;
            end if;  
          end if; 
        else
          p_string := '"<Unsupported type>"';
              
          if (i < p_col_count) then
            p_string := p_string || ';';  
          end if; 
          
          p_raw := utl_i18n.string_to_raw(p_string, 'AL32UTF8');
          dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);    
        end if;  
      end loop;  
            
      dbms_lob.writeappend(p_outlob, 2, p_newline); 
    end loop; 
     
    dbms_sql.close_cursor(p_cursor);     
    dbms_lob.freetemporary(p_tmpblob); 
    
    if p_compress then
      p_outlob := utl_compress.lz_compress(p_outlob); 
    end if; 
  end export2csv; 

  --------------------- PUBLIC -------------------------

  procedure export2csv(p_query    clob,
                       p_dir      varchar2,
                       p_file     varchar2,
                       p_header   boolean default true,
                       p_bom      boolean default true,
                       p_compress boolean default false) 
  as   
    p_cursor   number; 
    p_filename varchar2(1000) := p_file || case when p_compress then '.gz' end;
    p_cnt      pls_integer;
  begin    
    p_cursor := dbms_sql.open_cursor; 
    dbms_sql.parse(p_cursor, p_query, dbms_sql.native);
    p_cnt := dbms_sql.execute(p_cursor); 
    
    export2csv(p_cursor, p_dir, p_filename, p_header, p_bom, p_compress); 
  end export2csv; 
 
  procedure export2csv(p_refcursor in out sys_refcursor,
                       p_dir       varchar2,
                       p_file      varchar2,
                       p_header    boolean default true,
                       p_bom       boolean default true,
                       p_compress  boolean default false) 
  as   
    p_cursor   number := dbms_sql.to_cursor_number(p_refcursor);  
    p_filename varchar2(1000) := p_file || case when p_compress then '.gz' end; 
  begin     
    export2csv(p_cursor, p_dir, p_filename, p_header, p_bom, p_compress);  
  end export2csv; 
  
  procedure export2csv(p_query    clob,
                       p_outlob   out blob, 
                       p_header   boolean default true,
                       p_bom      boolean default true,
                       p_compress boolean default false) 
  as   
    p_cursor number := dbms_sql.open_cursor;      
    p_cnt    number;     
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    dbms_sql.parse(p_cursor, p_query, dbms_sql.native); 
    p_cnt := dbms_sql.execute(p_cursor);    
    export2csv(p_cursor, p_outlob, p_header, p_bom, p_compress);  
  end export2csv; 

  procedure export2csv(p_refcursor in out sys_refcursor,
                       p_outlob    out blob, 
                       p_header    boolean default true,
                       p_bom       boolean default true,
                       p_compress  boolean default false) 
  as   
    p_cursor number := dbms_sql.to_cursor_number(p_refcursor);   
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    export2csv(p_cursor, p_outlob, p_header, p_bom, p_compress);  
  end export2csv; 

end EXPIMP;
/
