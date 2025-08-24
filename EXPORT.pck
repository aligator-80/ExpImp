create or replace package EXPORT is

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
                       
  procedure export2xls(p_query    clob,
                       p_dir      varchar2,
                       p_file     varchar2, 
                       p_compress boolean default false);
                       
  procedure export2xls(p_refcursor in out sys_refcursor,
                       p_dir       varchar2,
                       p_file      varchar2, 
                       p_compress  boolean default false);

  procedure export2xls(p_query    clob,
                       p_outlob   out blob,  
                       p_compress boolean default false);
                       
  procedure export2xls(p_refcursor in out sys_refcursor,
                       p_outlob    out blob,  
                       p_compress  boolean default false);

  procedure export2json(p_query    clob,
                        p_dir      varchar2,
                        p_file     varchar2, 
                        p_bom      boolean default true,
                        p_compress boolean default false); 
 
  procedure export2json(p_refcursor in out sys_refcursor,
                        p_dir       varchar2,
                        p_file      varchar2, 
                        p_bom       boolean default true,
                        p_compress  boolean default false); 
  
  procedure export2json(p_query    clob,
                        p_outlob   out blob,  
                        p_bom      boolean default true,
                        p_compress boolean default false); 

  procedure export2json(p_refcursor in out sys_refcursor,
                        p_outlob    out blob,  
                        p_bom       boolean default true,
                        p_compress  boolean default false); 

end EXPORT;
/

create or replace package body EXPORT is

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

  procedure export2xls(p_cursor   in out integer,
                       p_dir      varchar2,
                       p_file     varchar2, 
                       p_compress boolean) 
  as                     
    type style_table_t is table of varchar2(10) index by pls_integer;       
    type style_table2_t is table of pls_integer index by varchar2(10);           
                        
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab; 
    p_tmpblob   blob;
    p_string    varchar2(32767);   
    p_number    number;  
    p_timestamp timestamp(6);
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob;   
    i           pls_integer := 0;
    j           pls_integer := 0;
    p_rowcnt    pls_integer := 1;
    p_length    pls_integer;   
    p_file_ptr  utl_file.file_type := utl_file.fopen(p_dir, p_file, 'wb');  
    p_styles    style_table_t; 
    p_unq_st    style_table2_t; 
    p_style_id  varchar2(10);  
    p_newline   char(2) := chr(13) || chr(10);    
    p_is_empty  boolean;
    p_endcell   raw(100) := utl_i18n.string_to_raw('</Data></Cell>' || p_newline, 'AL32UTF8');
    p_bfile     bfile := bfilename(p_dir, p_file);
    
    procedure write_clob(p_data     in out nocopy clob character set any_cs,
                         p_colindex pls_integer)
    as 
      p_strdata varchar2(32767) character set p_data%charset; 
    begin   
      if (dbms_lob.getlength(p_data) > 32767) then 
        raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
      end if;
      
      p_strdata := dbms_lob.substr(p_data, 32767, 1);
      
      begin
        p_strdata := replace(p_strdata, '<', '&lt;');
        p_strdata := replace(p_strdata, '>', '&gt;'); 
      exception
        when others then
          raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
      end;  
      
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || p_colindex || '"' end || 
                       '><Data ss:Type="String">', 'AL32UTF8'));  
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_strdata, 'AL32UTF8'));  
      utl_file.put_raw(p_file_ptr, p_endcell); 
    end write_clob;
  begin     
    utl_file.put_raw(p_file_ptr, 'EFBBBF'); 
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 	  xmlns:o="urn:schemas-microsoft-com:office:office"
 	  xmlns:x="urn:schemas-microsoft-com:office:excel"
 	  xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"> 
 <Styles> 
  <Style ss:ID="s01">
   <Borders>
    <Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>
   </Borders>
   <Font ss:Bold="1"/>
  </Style>
  <Style ss:ID="s02">
   <NumberFormat ss:Format="General Number"/>    
  </Style>
  <Style ss:ID="s03">
   <NumberFormat ss:Format="General Date"/>    
  </Style>
  <Style ss:ID="s04">
   <NumberFormat ss:Format="0.00000E+00"/>  
  </Style>
  <Style ss:ID="s05">
   <NumberFormat ss:Format="0.00000000000000E+00"/>  
  </Style>
', 'AL32UTF8'));   
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
            dbms_sql.define_column(p_cursor, i, p_number);   
            
            if (p_desc_tab(i).col_scale = -127) then
              p_styles(i) := 's02'; 
            elsif (p_desc_tab(i).col_type = 100) then
              p_styles(i) := 's04'; 
            elsif (p_desc_tab(i).col_type = 101) then
              p_styles(i) := 's05'; 
            else  
              p_style_id := 'n' || p_desc_tab(i).col_scale; 
              
              if not p_unq_st.exists(p_style_id) then
                p_unq_st(p_style_id) := 1;
              end if;
            
              p_styles(i) := p_style_id; 
            end if;   
          elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
            dbms_sql.define_column(p_cursor, i, p_timestamp);  
            p_styles(i) := 's03';   
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if;  
    end loop;
    
    p_string := p_unq_st.first; 
    
    while p_string is not null loop
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('  <Style ss:ID="' || p_string || '">
   <NumberFormat ss:Format="0' || case when p_string != 'n0' then '.' || lpad('0', to_number(substr(p_string, 2)), '0') end || '"/>  
  </Style>' || p_newline, 'AL32UTF8'));   
     
      p_string := p_unq_st.next(p_string);
    end loop;  
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(' </Styles>
 <Worksheet ss:Name="DATA">
  <Table>' || p_newline, 'AL32UTF8'));   

    p_number := p_styles.first; 
    
    while p_number is not null loop
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('    <Column ss:Index="' || p_number || '" ss:StyleID="' || p_styles(p_number) || '"/>' || p_newline, 'AL32UTF8'));   
     
      p_number := p_styles.next(p_number);
    end loop;   
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('    <Row ss:StyleID="s01">' || p_newline, 'AL32UTF8'));   
    
    for i in 1..p_col_count loop   
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell><Data ss:Type="String">' || p_desc_tab(i).col_name || 
                       '</Data></Cell>' || p_newline, 'AL32UTF8'));  
    end loop;  
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('    </Row>' || p_newline, 'AL32UTF8'));   
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
      
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('    <Row>' || p_newline, 'AL32UTF8'));  
      p_is_empty := false;  
    
      for i in 1..p_col_count loop  
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then
            dbms_sql.column_value(p_cursor, i, p_nclob);   
            
            if nvl(dbms_lob.getlength(p_nclob), 0) = 0 then
              p_is_empty := true;
              continue;  
            end if;
            
            PRAGMA INLINE (write_clob, 'YES');
            write_clob(p_nclob, i); 
            
            p_is_empty := false;
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob); 
              
              if nvl(dbms_lob.getlength(p_clob), 0) = 0 then
                p_is_empty := true;
                continue;  
              end if;
              
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_clob, i);
               
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
              dbms_sql.column_value(p_cursor, i, p_number);    
              
              if p_number is null then
                p_is_empty := true;
                continue;  
              end if;
              
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                               '><Data ss:Type="Number">', 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(to_char(p_number, 'TM9', 'NLS_NUMERIC_CHARACTERS = ''.,'''), 'AL32UTF8') || p_endcell);  
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
              dbms_sql.column_value(p_cursor, i, p_timestamp);   
              
              if p_timestamp is null then
                p_is_empty := true;
                continue;  
              end if;
              
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                               '><Data ss:Type="DateTime">', 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(to_char(p_timestamp, 'yyyy-mm-dd"T"hh24:mi:ss.ff3'), 'AL32UTF8') || p_endcell);   
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type = 8) then   
              dbms_sql.column_value_long(p_cursor, i, 32767, 0, p_string, p_length);   
              
              if p_length = 0 then
                p_is_empty := true;
                continue; 
              end if;
              
              if (p_length = 32767) then
                dbms_sql.column_value_long(p_cursor, i, 1, 32767, p_string, p_length);   
                
                if (p_length > 0) then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded'); 
                end if;
              end if;
              
              begin
                p_string := replace(p_string, '<', '&lt;');
                p_string := replace(p_string, '>', '&gt;'); 
              exception
                when others then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end;  
            
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                               '><Data ss:Type="String">', 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_string, 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, p_endcell); 
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);  
              
              p_length := dbms_lob.getlength(p_blob);
              
              if nvl(p_length, 0) = 0 then
                p_is_empty := true;
                continue;  
              end if;
              
              if (p_length > 16383) then 
                raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end if;
              
              p_string := rawtohex(dbms_lob.substr(p_blob, 32767, 1)); 
              
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                               '><Data ss:Type="String">', 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_string, 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, p_endcell); 
              
              p_is_empty := false;
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              
              if p_string is null then
                p_is_empty := true;
                continue;  
              end if;
              
              begin
                p_string := replace(p_string, '<', '&lt;');
                p_string := replace(p_string, '>', '&gt;'); 
              exception
                when others then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end;     
              
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                               '><Data ss:Type="String">', 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(p_string, 'AL32UTF8'));  
              utl_file.put_raw(p_file_ptr, p_endcell);  
              
              p_is_empty := false;
            end if;  
          end if; 
        else
          utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                           '><Data ss:Type="String">&lt;Unsupported type&gt;</Data></Cell>' || p_newline, 'AL32UTF8'));     
              
          p_is_empty := false;
        end if;  
      end loop;  
          
      utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('    </Row>' || p_newline, 'AL32UTF8'));
      
      p_rowcnt := p_rowcnt + 1;
      
      if (p_rowcnt > 1048576) then
        raise_application_error(-20002, 'Excel maximum row (1048576) exceeded');  
      end if;
    end loop; 
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('  </Table>
  <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel"> 
   <Selected/>
   <FreezePanes/>
   <SplitHorizontal>1</SplitHorizontal>
   <TopRowBottomPane>1</TopRowBottomPane>
   <ActivePane>2</ActivePane>  
  </WorksheetOptions>
 </Worksheet>
</Workbook>', 'AL32UTF8'));  
     
    dbms_sql.close_cursor(p_cursor);   
    utl_file.fclose(p_file_ptr);   
    
    if p_compress then 
      dbms_lob.createtemporary(p_tmpblob, true);   
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
      dbms_lob.freetemporary(p_tmpblob);
    end if; 
  end export2xls; 

  procedure export2xls(p_cursor   in out integer,
                       p_outlob   in out blob, 
                       p_compress boolean) 
  as                     
    type style_table_t is table of varchar2(10) index by pls_integer;       
    type style_table2_t is table of pls_integer index by varchar2(10);           
                        
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab;  
    p_raw       raw(32767);
    p_string    varchar2(32767);   
    p_number    number;  
    p_timestamp timestamp(6);
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob;   
    i           pls_integer := 0; 
    p_rowcnt    pls_integer := 1;
    p_length    pls_integer;     
    p_styles    style_table_t; 
    p_unq_st    style_table2_t; 
    p_style_id  varchar2(10);  
    p_newline   char(2) := chr(13) || chr(10);    
    p_is_empty  boolean;
    p_endcell   raw(100) := utl_i18n.string_to_raw('</Data></Cell>' || p_newline, 'AL32UTF8'); 
    
    procedure write_clob(p_data     in out nocopy clob character set any_cs,
                         p_colindex pls_integer)
    as 
      p_strdata varchar2(32767) character set p_data%charset; 
    begin   
      if (dbms_lob.getlength(p_data) > 32767) then 
        raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
      end if;
      
      p_strdata := dbms_lob.substr(p_data, 32767, 1);
      
      begin
        p_strdata := replace(p_strdata, '<', '&lt;');
        p_strdata := replace(p_strdata, '>', '&gt;'); 
      exception
        when others then
          raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
      end;  
      
      p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || p_colindex || '"' end || 
                                      '><Data ss:Type="String">', 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
      p_raw := utl_i18n.string_to_raw(p_strdata, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
      dbms_lob.writeappend(p_outlob, 16, p_endcell);  
    end write_clob;
  begin     
    dbms_lob.createtemporary(p_outlob, true);  
    dbms_lob.writeappend(p_outlob, 3, 'EFBBBF');  
    
    p_raw := utl_i18n.string_to_raw('<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 	  xmlns:o="urn:schemas-microsoft-com:office:office"
 	  xmlns:x="urn:schemas-microsoft-com:office:excel"
 	  xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"> 
 <Styles> 
  <Style ss:ID="s01">
   <Borders>
    <Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>
   </Borders>
   <Font ss:Bold="1"/>
  </Style>
  <Style ss:ID="s02">
   <NumberFormat ss:Format="General Number"/>    
  </Style>
  <Style ss:ID="s03">
   <NumberFormat ss:Format="General Date"/>    
  </Style>
  <Style ss:ID="s04">
   <NumberFormat ss:Format="0.00000E+00"/>  
  </Style>
  <Style ss:ID="s05">
   <NumberFormat ss:Format="0.00000000000000E+00"/>  
  </Style>
', 'AL32UTF8');
    dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
            dbms_sql.define_column(p_cursor, i, p_number);   
            
            if (p_desc_tab(i).col_scale = -127) then
              p_styles(i) := 's02'; 
            elsif (p_desc_tab(i).col_type = 100) then
              p_styles(i) := 's04'; 
            elsif (p_desc_tab(i).col_type = 101) then
              p_styles(i) := 's05'; 
            else  
              p_style_id := 'n' || p_desc_tab(i).col_scale; 
              
              if not p_unq_st.exists(p_style_id) then
                p_unq_st(p_style_id) := 1;
              end if;
            
              p_styles(i) := p_style_id; 
            end if;   
          elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
            dbms_sql.define_column(p_cursor, i, p_timestamp);  
            p_styles(i) := 's03';   
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if;  
    end loop;
    
    p_string := p_unq_st.first; 
    
    while p_string is not null loop
      p_raw := utl_i18n.string_to_raw('  <Style ss:ID="' || p_string || '">
   <NumberFormat ss:Format="0' || case when p_string != 'n0' then '.' || lpad('0', to_number(substr(p_string, 2)), '0') end || '"/>  
  </Style>' || p_newline, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);    
     
      p_string := p_unq_st.next(p_string);
    end loop;  
    
    p_raw := utl_i18n.string_to_raw(' </Styles>
 <Worksheet ss:Name="DATA">
  <Table>' || p_newline, 'AL32UTF8');
    dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  

    p_number := p_styles.first; 
    
    while p_number is not null loop
      p_raw := utl_i18n.string_to_raw('    <Column ss:Index="' || p_number || '" ss:StyleID="' || p_styles(p_number) || '"/>' || p_newline, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
     
      p_number := p_styles.next(p_number);
    end loop;   
    
    p_raw := utl_i18n.string_to_raw('    <Row ss:StyleID="s01">' || p_newline, 'AL32UTF8');
    dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
    
    for i in 1..p_col_count loop   
      p_raw := utl_i18n.string_to_raw('      <Cell><Data ss:Type="String">' || p_desc_tab(i).col_name || 
                                      '</Data></Cell>' || p_newline, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
    end loop;  
    
    p_raw := utl_i18n.string_to_raw('    </Row>' || p_newline, 'AL32UTF8');
    dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
      
      p_raw := utl_i18n.string_to_raw('    <Row>' || p_newline, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
       
      p_is_empty := false;  
    
      for i in 1..p_col_count loop  
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then
            dbms_sql.column_value(p_cursor, i, p_nclob);   
            
            if nvl(dbms_lob.getlength(p_nclob), 0) = 0 then
              p_is_empty := true;
              continue;  
            end if;
            
            PRAGMA INLINE (write_clob, 'YES');
            write_clob(p_nclob, i); 
            
            p_is_empty := false;
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob); 
              
              if nvl(dbms_lob.getlength(p_clob), 0) = 0 then
                p_is_empty := true;
                continue;  
              end if;
              
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_clob, i);
               
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
              dbms_sql.column_value(p_cursor, i, p_number);    
              
              if p_number is null then
                p_is_empty := true;
                continue;  
              end if;
              
              p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                              '><Data ss:Type="Number">', 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              p_raw := utl_i18n.string_to_raw(to_char(p_number, 'TM9', 'NLS_NUMERIC_CHARACTERS = ''.,'''), 'AL32UTF8') || p_endcell;
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
              dbms_sql.column_value(p_cursor, i, p_timestamp);   
              
              if p_timestamp is null then
                p_is_empty := true;
                continue;  
              end if;
              
              p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                              '><Data ss:Type="DateTime">', 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
              p_raw := utl_i18n.string_to_raw(to_char(p_timestamp, 'yyyy-mm-dd"T"hh24:mi:ss.ff3'), 'AL32UTF8') || p_endcell;
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type = 8) then   
              dbms_sql.column_value_long(p_cursor, i, 32767, 0, p_string, p_length);   
              
              if p_length = 0 then
                p_is_empty := true;
                continue; 
              end if;
              
              if (p_length = 32767) then
                dbms_sql.column_value_long(p_cursor, i, 1, 32767, p_string, p_length);   
                
                if (p_length > 0) then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded'); 
                end if;
              end if;
              
              begin
                p_string := replace(p_string, '<', '&lt;');
                p_string := replace(p_string, '>', '&gt;'); 
              exception
                when others then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end;  
              
              p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                              '><Data ss:Type="String">', 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
              p_raw := utl_i18n.string_to_raw(p_string, 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
              dbms_lob.writeappend(p_outlob, 16, p_endcell);   
              
              p_is_empty := false;
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);  
              
              p_length := dbms_lob.getlength(p_blob);
              
              if nvl(p_length, 0) = 0 then
                p_is_empty := true;
                continue;  
              end if;
              
              if (p_length > 16383) then 
                raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end if;
              
              p_string := rawtohex(dbms_lob.substr(p_blob, 32767, 1)); 
              
              p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                              '><Data ss:Type="String">', 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              p_raw := utl_i18n.string_to_raw(p_string, 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              dbms_lob.writeappend(p_outlob, 16, p_endcell); 
              
              p_is_empty := false;
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              
              if p_string is null then
                p_is_empty := true;
                continue;  
              end if;
              
              begin
                p_string := replace(p_string, '<', '&lt;');
                p_string := replace(p_string, '>', '&gt;'); 
              exception
                when others then
                  raise_application_error(-20001, 'Character limit per cell (32767) exceeded');  
              end;     
              
              p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                              '><Data ss:Type="String">', 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              p_raw := utl_i18n.string_to_raw(p_string, 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              dbms_lob.writeappend(p_outlob, 16, p_endcell);  
              
              p_is_empty := false;
            end if;  
          end if; 
        else
          p_raw := utl_i18n.string_to_raw('      <Cell' || case when p_is_empty then ' ss:Index="' || i || '"' end || 
                                          '><Data ss:Type="String">&lt;Unsupported type&gt;</Data></Cell>' || p_newline, 'AL32UTF8');
          dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw); 
              
          p_is_empty := false;
        end if;  
      end loop;  
          
      p_raw := utl_i18n.string_to_raw('    </Row>' || p_newline, 'AL32UTF8');
      dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
      
      p_rowcnt := p_rowcnt + 1;
      
      if (p_rowcnt > 1048576) then
        raise_application_error(-20002, 'Excel maximum row (1048576) exceeded');  
      end if;
    end loop; 
    
    p_raw := utl_i18n.string_to_raw('  </Table>
  <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel"> 
   <Selected/>
   <FreezePanes/>
   <SplitHorizontal>1</SplitHorizontal>
   <TopRowBottomPane>1</TopRowBottomPane>
   <ActivePane>2</ActivePane>  
  </WorksheetOptions>
 </Worksheet>
</Workbook>', 'AL32UTF8');
    dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
     
    dbms_sql.close_cursor(p_cursor);    
    
    if p_compress then
      p_outlob := utl_compress.lz_compress(p_outlob); 
    end if; 
  end export2xls; 
  
  procedure export2json(p_cursor   in out integer,
                        p_dir      varchar2,
                        p_file     varchar2, 
                        p_bom      boolean,
                        p_compress boolean) 
  as 
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab; 
    p_number    number;
    p_string    varchar2(32767);  
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob; 
    p_tclob     nclob;
    p_tmpblob   blob;
    p_tmpclob   clob;
    p_timestamp timestamp;
    p_cnt       number;   
    i           pls_integer := 0;
    j           pls_integer := 0;
    p_length    pls_integer; 
    p_lbracket  raw(1) := utl_i18n.string_to_raw('{', 'AL32UTF8'); 
    p_rbracket  raw(1) := utl_i18n.string_to_raw('}', 'AL32UTF8'); 
    p_coma      raw(1) := utl_i18n.string_to_raw(',', 'AL32UTF8');   
    p_file_ptr  utl_file.file_type := utl_file.fopen(p_dir, p_file, 'wb'); 
    p_bfile     bfile := bfilename(p_dir, p_file);
    p_start     boolean := true;
    p_tmp_c     sys_refcursor;
    
    procedure write_clob(p_data     in out nocopy clob character set any_cs,
                         p_is_nchar boolean,
                         p_index    pls_integer)
    as 
      p_dest_offset  pls_integer := 1;
      p_src_offset   pls_integer := 1;
      p_warning      pls_integer; 
      p_lang_context pls_integer := dbms_lob.default_lang_ctx;
    begin   
      if p_is_nchar then
        p_tclob := p_data;
        j := 0;
        p_length := dbms_lob.getlength(p_tclob);
        dbms_lob.trim(p_tmpclob, 0);
              
        while (j < p_length) loop 
          p_tmpclob := p_tmpclob || asciistr(substr(p_tclob, j + 1, least(p_length - j, 4000)));  
          j := j + 4000;
        end loop; 
          
        p_string := 'select replace(JSON_OBJECT(asciistr(:1) value :2 returning clob), ''\\'', ''\u'') from dual';
        open p_tmp_c for p_string using p_desc_tab(p_index).col_name, p_tmpclob;
        fetch p_tmp_c into p_tmpclob;  
      else
        p_string := 'select JSON_OBJECT(:1 value :2 returning clob) from dual';
        open p_tmp_c for p_string using p_desc_tab(p_index).col_name, p_data;
        fetch p_tmp_c into p_tmpclob;     
      end if; 
               
      dbms_lob.trim(p_tmpblob, 0);
            
      dbms_lob.convertToBlob(dest_lob     => p_tmpblob,
                             src_clob     => substr(p_tmpclob, 2, length(p_tmpclob) - 2),
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
    dbms_lob.createtemporary(p_tmpclob, true);   
    
    if p_bom then
      utl_file.put_raw(p_file_ptr, 'EFBBBF');
    end if; 
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw('[', 'AL32UTF8'));
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
            dbms_sql.define_column(p_cursor, i, p_number);  
          elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
            dbms_sql.define_column(p_cursor, i, p_timestamp);   
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if; 
    end loop; 
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
      
      if not p_start then
        utl_file.put_raw(p_file_ptr, p_coma); 
      else
        p_start := false;
      end if;
      
      utl_file.put_raw(p_file_ptr, p_lbracket);
    
      for i in 1..p_col_count loop 
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then  
            dbms_sql.column_value(p_cursor, i, p_nclob);
            
            p_length := nvl(dbms_lob.getlength(p_nclob), 0); 
            
            if p_length > 0 then
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_nclob, true, i); 
            else 
              p_string := json_object(p_desc_tab(i).col_name value null);   
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));  
            end if;   
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob);  
              
              if dbms_lob.getlength(p_clob) > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_clob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);   
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));    
              end if;  
            elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
              dbms_sql.column_value(p_cursor, i, p_number);   
              p_string := json_object(p_desc_tab(i).col_name value p_number);   
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));    
            elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
              dbms_sql.column_value(p_cursor, i, p_timestamp);     
              p_string := json_object(p_desc_tab(i).col_name value p_timestamp);   
              utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));  
            elsif (p_desc_tab(i).col_type = 8) then  
              p_cnt := 0;
              dbms_lob.trim(p_tmpclob, 0);
              dbms_sql.column_value_long(p_cursor, i, 32767, p_cnt, p_string, p_length); 
            
              while (p_length > 0) loop  
                p_tmpclob := p_tmpclob || p_string;  
                p_cnt := p_cnt + p_length;
                dbms_sql.column_value_long(p_cursor, i, 32767, p_cnt, p_string, p_length); 
              end loop; 
              
              if p_cnt > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);   
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));   
              end if; 
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);   
                 
              p_length := nvl(dbms_lob.getlength(p_blob), 0); 
              
              j := 0; 
              dbms_lob.trim(p_tmpclob, 0);
              
              while (j < p_length) loop 
                p_tmpclob := p_tmpclob || rawtohex(dbms_lob.substr(p_blob, least(p_length - j, 32767), j + 1)); 
                j := j + 32767;
              end loop; 
              
              if p_length > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);   
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));   
              end if;  
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              p_length := nvl(length(p_string), 0);   
              
              if (p_length > 16000) then 
                dbms_lob.trim(p_tmpclob, 0);
                dbms_lob.writeappend(p_tmpclob, 1, p_string); 
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value p_string);   
                utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));
              end if;
            end if;  
          end if; 
        else
          p_string := json_object(p_desc_tab(i).col_name value '<Unsupported type>');   
          utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8'));  
        end if;   
      
        if (i < p_col_count) then
          utl_file.put_raw(p_file_ptr, p_coma);  
        end if;
      end loop;  
          
      utl_file.put_raw(p_file_ptr, p_rbracket); 
    end loop; 
    
    utl_file.put_raw(p_file_ptr, utl_i18n.string_to_raw(']', 'AL32UTF8'));
     
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
    dbms_lob.freetemporary(p_tmpclob);    
  end export2json;

  procedure export2json(p_cursor   in out integer,
                        p_outlob   in out blob, 
                        p_bom      boolean,
                        p_compress boolean) 
  as 
    p_col_count number;
    p_desc_tab  dbms_sql.desc_tab; 
    p_raw       raw(32767);
    p_number    number;
    p_string    varchar2(32767);  
    p_clob      clob; 
    p_blob      blob;
    p_nclob     nclob; 
    p_tclob     nclob;
    p_tmpblob   blob;
    p_tmpclob   clob;
    p_timestamp timestamp;
    p_cnt       number;   
    i           pls_integer := 0;
    j           pls_integer := 0;
    p_length    pls_integer; 
    p_lbracket  raw(1) := utl_i18n.string_to_raw('{', 'AL32UTF8'); 
    p_rbracket  raw(1) := utl_i18n.string_to_raw('}', 'AL32UTF8'); 
    p_coma      raw(1) := utl_i18n.string_to_raw(',', 'AL32UTF8');    
    p_start     boolean := true;
    p_tmp_c     sys_refcursor;
    
    procedure write_clob(p_data     in out nocopy clob character set any_cs,
                         p_is_nchar boolean,
                         p_index    pls_integer)
    as 
      p_dest_offset  pls_integer := 1;
      p_src_offset   pls_integer := 1;
      p_warning      pls_integer; 
      p_lang_context pls_integer := dbms_lob.default_lang_ctx;
    begin   
      if p_is_nchar then
        p_tclob := p_data;
        j := 0;
        p_length := dbms_lob.getlength(p_tclob);
        dbms_lob.trim(p_tmpclob, 0);
              
        while (j < p_length) loop 
          p_tmpclob := p_tmpclob || asciistr(substr(p_tclob, j + 1, least(p_length - j, 4000)));  
          j := j + 4000;
        end loop; 
          
        p_string := 'select replace(JSON_OBJECT(asciistr(:1) value :2 returning clob), ''\\'', ''\u'') from dual';
        open p_tmp_c for p_string using p_desc_tab(p_index).col_name, p_tmpclob;
        fetch p_tmp_c into p_tmpclob;  
      else
        p_string := 'select JSON_OBJECT(:1 value :2 returning clob) from dual';
        open p_tmp_c for p_string using p_desc_tab(p_index).col_name, p_data;
        fetch p_tmp_c into p_tmpclob;     
      end if; 
               
      dbms_lob.trim(p_tmpblob, 0);
            
      dbms_lob.convertToBlob(dest_lob     => p_tmpblob,
                             src_clob     => substr(p_tmpclob, 2, length(p_tmpclob) - 2),
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
    dbms_lob.createtemporary(p_tmpclob, true);   
    
    if p_bom then
      dbms_lob.writeappend(p_outlob, 3, 'EFBBBF'); 
    end if; 
    
    dbms_lob.writeappend(p_outlob, 1, utl_i18n.string_to_raw('[', 'AL32UTF8'));  
    
    dbms_sql.describe_columns(p_cursor, p_col_count, p_desc_tab);   
    
    for i in 1..p_col_count loop  
      if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
        if (p_desc_tab(i).col_charsetform = 2) then
          dbms_sql.define_column(p_cursor, i, p_nclob);  
        else
          if (p_desc_tab(i).col_type = 112) then  
            dbms_sql.define_column(p_cursor, i, p_clob);  
          elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
            dbms_sql.define_column(p_cursor, i, p_number);  
          elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
            dbms_sql.define_column(p_cursor, i, p_timestamp);   
          elsif (p_desc_tab(i).col_type in (23, 113)) then 
            dbms_sql.define_column(p_cursor, i, p_blob);  
          elsif (p_desc_tab(i).col_type = 8) then 
            dbms_sql.define_column_long(p_cursor, i);  
          else
            dbms_sql.define_column(p_cursor, i, p_string, 32767);  
          end if;  
        end if;
      end if; 
    end loop; 
    
    loop
      exit when dbms_sql.fetch_rows(p_cursor) = 0;  
      
      if not p_start then
        dbms_lob.writeappend(p_outlob, 1, p_coma);   
      else
        p_start := false;
      end if;
       
      dbms_lob.writeappend(p_outlob, 1, p_lbracket);   
    
      for i in 1..p_col_count loop 
        if (p_desc_tab(i).col_type in (1, 2, 8, 12, 100, 101, 180, 181, 231, 182, 183, 23, 69, 208, 96, 112, 113)) then
          if (p_desc_tab(i).col_charsetform = 2) then  
            dbms_sql.column_value(p_cursor, i, p_nclob);
            
            p_length := nvl(dbms_lob.getlength(p_nclob), 0); 
            
            if p_length > 0 then
              PRAGMA INLINE (write_clob, 'YES');
              write_clob(p_nclob, true, i); 
            else  
              p_string := json_object(p_desc_tab(i).col_name value null);   
              p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);  
            end if;   
          else
            if (p_desc_tab(i).col_type = 112) then  
              dbms_sql.column_value(p_cursor, i, p_clob);  
              
              if dbms_lob.getlength(p_clob) > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_clob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);   
                p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);     
              end if;  
            elsif (p_desc_tab(i).col_type in (2, 100, 101)) then 
              dbms_sql.column_value(p_cursor, i, p_number);   
              p_string := json_object(p_desc_tab(i).col_name value p_number);   
              p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);   
            elsif (p_desc_tab(i).col_type in (12, 180, 231)) then 
              dbms_sql.column_value(p_cursor, i, p_timestamp);     
              p_string := json_object(p_desc_tab(i).col_name value p_timestamp);      
              p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
              dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);     
            elsif (p_desc_tab(i).col_type = 8) then  
              p_cnt := 0;
              dbms_lob.trim(p_tmpclob, 0);
              dbms_sql.column_value_long(p_cursor, i, 32767, p_cnt, p_string, p_length); 
            
              while (p_length > 0) loop  
                p_tmpclob := p_tmpclob || p_string;  
                p_cnt := p_cnt + p_length;
                dbms_sql.column_value_long(p_cursor, i, 32767, p_cnt, p_string, p_length); 
              end loop; 
              
              if p_cnt > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);      
                p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);    
              end if; 
            elsif (p_desc_tab(i).col_type in (23, 113)) then 
              dbms_sql.column_value(p_cursor, i, p_blob);   
                 
              p_length := nvl(dbms_lob.getlength(p_blob), 0); 
              
              j := 0; 
              dbms_lob.trim(p_tmpclob, 0);
              
              while (j < p_length) loop 
                p_tmpclob := p_tmpclob || rawtohex(dbms_lob.substr(p_blob, least(p_length - j, 32767), j + 1)); 
                j := j + 32767;
              end loop; 
              
              if p_length > 0 then
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value null);    
                p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);       
              end if;  
            else
              dbms_sql.column_value(p_cursor, i, p_string); 
              p_length := nvl(length(p_string), 0);   
              
              if (p_length > 16000) then 
                dbms_lob.trim(p_tmpclob, 0);
                dbms_lob.writeappend(p_tmpclob, 1, p_string); 
                PRAGMA INLINE (write_clob, 'YES');
                write_clob(p_tmpclob, false, i); 
              else 
                p_string := json_object(p_desc_tab(i).col_name value p_string);      
                p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
                dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);    
              end if;
            end if;  
          end if; 
        else
          p_string := json_object(p_desc_tab(i).col_name value '<Unsupported type>');      
          p_raw := utl_i18n.string_to_raw(substr(p_string, 2, length(p_string) - 2), 'AL32UTF8');
          dbms_lob.writeappend(p_outlob, utl_raw.length(p_raw), p_raw);    
        end if;   
      
        if (i < p_col_count) then    
          dbms_lob.writeappend(p_outlob, 1, p_coma);   
        end if;
      end loop;  
            
      dbms_lob.writeappend(p_outlob, 1, p_rbracket); 
    end loop; 
    
    dbms_lob.writeappend(p_outlob, 1, utl_i18n.string_to_raw(']', 'AL32UTF8'));  
     
    dbms_sql.close_cursor(p_cursor);   
    
    if p_compress then 
       p_outlob := utl_compress.lz_compress(p_outlob);   
    end if; 
    
    dbms_lob.freetemporary(p_tmpblob); 
    dbms_lob.freetemporary(p_tmpclob);    
  end export2json;

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

  procedure export2xls(p_query    clob,
                       p_dir      varchar2,
                       p_file     varchar2, 
                       p_compress boolean default false) 
  as   
    p_cursor   number; 
    p_filename varchar2(1000) := p_file || case when p_compress then '.gz' end;
    p_cnt      pls_integer;
  begin    
    p_cursor := dbms_sql.open_cursor; 
    dbms_sql.parse(p_cursor, p_query, dbms_sql.native);
    p_cnt := dbms_sql.execute(p_cursor); 
    
    export2xls(p_cursor, p_dir, p_filename, p_compress); 
  end export2xls; 
 
  procedure export2xls(p_refcursor in out sys_refcursor,
                       p_dir       varchar2,
                       p_file      varchar2, 
                       p_compress  boolean default false) 
  as   
    p_cursor   number := dbms_sql.to_cursor_number(p_refcursor);  
    p_filename varchar2(1000) := p_file || case when p_compress then '.gz' end; 
  begin     
    export2xls(p_cursor, p_dir, p_filename, p_compress);  
  end export2xls; 
  
  procedure export2xls(p_query    clob,
                       p_outlob   out blob,  
                       p_compress boolean default false) 
  as   
    p_cursor number := dbms_sql.open_cursor;      
    p_cnt    number;     
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    dbms_sql.parse(p_cursor, p_query, dbms_sql.native); 
    p_cnt := dbms_sql.execute(p_cursor);    
    export2xls(p_cursor, p_outlob, p_compress);  
  end export2xls; 

  procedure export2xls(p_refcursor in out sys_refcursor,
                       p_outlob    out blob,  
                       p_compress  boolean default false) 
  as   
    p_cursor number := dbms_sql.to_cursor_number(p_refcursor);   
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    export2xls(p_cursor, p_outlob, p_compress);  
  end export2xls;  

  procedure export2json(p_query    clob,
                        p_dir      varchar2,
                        p_file     varchar2, 
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
    
    export2json(p_cursor, p_dir, p_filename, p_bom, p_compress); 
  end export2json; 
 
  procedure export2json(p_refcursor in out sys_refcursor,
                        p_dir       varchar2,
                        p_file      varchar2, 
                        p_bom       boolean default true,
                        p_compress  boolean default false) 
  as   
    p_cursor   number := dbms_sql.to_cursor_number(p_refcursor);  
    p_filename varchar2(1000) := p_file || case when p_compress then '.gz' end; 
  begin     
    export2json(p_cursor, p_dir, p_filename, p_bom, p_compress);  
  end export2json; 
  
  procedure export2json(p_query    clob,
                        p_outlob   out blob,  
                        p_bom      boolean default true,
                        p_compress boolean default false) 
  as   
    p_cursor number := dbms_sql.open_cursor;      
    p_cnt    number;     
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    dbms_sql.parse(p_cursor, p_query, dbms_sql.native); 
    p_cnt := dbms_sql.execute(p_cursor);    
    export2json(p_cursor, p_outlob, p_bom, p_compress);  
  end export2json; 

  procedure export2json(p_refcursor in out sys_refcursor,
                        p_outlob    out blob,  
                        p_bom       boolean default true,
                        p_compress  boolean default false) 
  as   
    p_cursor number := dbms_sql.to_cursor_number(p_refcursor);   
  begin     
    dbms_lob.createtemporary(p_outlob, true);   
    export2json(p_cursor, p_outlob, p_bom, p_compress);  
  end export2json; 

end EXPORT;
/
