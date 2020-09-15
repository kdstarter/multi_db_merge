namespace :db_merger do
  desc 'Check DB and update Conflict IDs'
  task check_tables: [:environment] do
    @apply_fix = false
    @reserved_rows = 1
    @DEFAULT_ID = 'id'
    start_time = Time.now.to_f.round(3)
    DbClient.log_by :info, "Start from #{start_time}."

    @tables = { larger: [], smaller: [], expect: [] }
    master_table = [@DEFAULT_ID, '']
    # master_table = ['email', 'members']
    count_expect_tables(master_table[0], master_table[1])
    update_tables_conflict_id(:smaller, master_table[0])

    end_time = Time.now.to_f.round(3)
    time_spent = (end_time - start_time).round(3)
    DbClient.log_by :info, "Total check spent #{time_spent} Seconds, end at #{end_time}.\n"
  end

  # --- begin 更新冲突的ID ---
  def update_tables_conflict_id(which_db, id_column)
    num = 1
    @to_do_columns.each do |table_name, asso|
      # 按冲突ID更新主表、以及关联表
      conflict_ids = asso[:conflict_ids]
      id_column_type = column_info(table_name, id_column)['udt_name']
      DbClient.log_by :info, "---small table#{num} #{table_name}, #{conflict_ids.size} conflict_#{id_column}s, #{asso[:foreign_keys].size} foreign_keys: #{asso[:foreign_keys]}"

      if id_column_type.start_with?('int')
        conflict_ids.each_with_index do |old_id, index|
          update_conflict_id(which_db, table_name, id_column, old_id, index + 1, asso[:foreign_keys])
        end
      else
        # Todo
        DbClient.log_by :warn, "Skip #{table_name} conflict_#{id_column}s for asso #{asso[:foreign_keys]}"
      end
      num += 1
    end
  end

  # table_name 主表名，asso_tables 关联表的表名，与外键名
  def update_conflict_id(which_db, table_name, id_column, old_id, index, asso_tables = [])
    if table_info(:larger, table_name)['rows'] < table_info(:smaller, table_name)['rows']
      larger_table = table_info(:smaller, table_name)
    else
      larger_table = table_info(:larger, table_name)
    end
    curr_new_id = larger_table['max_id'] + index + @reserved_rows

    ActiveRecord::Base.transaction do
      asso_tables.each_with_index do |table, asso_index|
        # 单个关联表
        asso_table = table.keys.first
        foreign_key = table.values.first
        
        if @apply_fix
          asso_sql = "UPDATE #{asso_table} SET #{foreign_key}=#{curr_new_id} WHERE #{foreign_key}=#{old_id}"
        else
          asso_sql = "SELECT #{foreign_key} FROM #{asso_table} WHERE #{foreign_key}=#{old_id}"
        end
        asso_result = DbClient.query(which_db, asso_sql)
        asso_rows = asso_result.cmd_tuples

        if index <= 2
          DbClient.log_sql :info, "ASSO#{asso_index} SQL#{index}: #{asso_table} update #{asso_rows} rows from ID#{old_id} to #{curr_new_id}, largerMaxId #{larger_table['max_id']}, #{asso_sql}"
        elsif index < 10
          DbClient.log_sql :info, "A#{old_id}."
        end
      end

      # 同步修改主表ID
      if @apply_fix
        main_sql = "UPDATE #{table_name} SET #{id_column}=#{curr_new_id} WHERE #{id_column}=#{old_id}"
      else
        main_sql = "SELECT #{id_column} FROM #{table_name} WHERE #{id_column}=#{old_id}"
      end
      main_result = DbClient.query(which_db, main_sql)
      main_rows = main_result.cmd_tuples

      expect_main_rows = 1 # @to_do_columns[table_name][:conflict_ids].size
      if main_rows != expect_main_rows
        error = "MAIN SQL#{index}: #{table_name} expect #{expect_main_rows} rows, but #{main_rows} rows"
        DbClient.log_by :error, "Error #{error}"
        raise error
      end

      if index <= 2
        DbClient.log_sql :info, "MAIN SQL#{index}: #{table_name} update #{main_rows} rows from ID#{old_id} to #{curr_new_id}, largerMaxId #{larger_table['max_id']}, #{main_sql}"
        DbClient.log_sql :info, '' if index == 0
      elsif index < 10
        DbClient.log_sql :info, "M#{old_id}."
      end
    end
  end
  # --- end 更新冲突的ID ---

  # --- begin 检查冲突的ID ---
  # 以下用于合并前的数据检查，与信息记录
  def count_expect_tables(id_column, master_table_name = '')
    tables_larger = tables_info(:larger, id_column)
    tables_smaller = tables_info(:smaller, id_column)
    get_tables_id_columns(:smaller, id_column)

    if master_table_name.present?
      master_table = table_info(:smaller, master_table_name)
      count_expect_table_by_id(master_table, id_column)
    else
      tables_smaller.each do |table|
        count_expect_table_by_id(table, id_column)
      end
    end
    
    # DbClient.log_by :info, "---smaller #{@tables[:smaller].size} tables---"
    # DbClient.log_by :info, @tables[:smaller]
    # DbClient.log_by :info, "---larger #{@tables[:larger].size} tables---"
    # DbClient.log_by :info, @tables[:larger]
    DbClient.log_by :info, "---expect #{@tables[:expect].size} tables---"
    DbClient.log_by :info, @tables[:expect]
    DbClient.log_by :info, "---schemas #{@schemas_by_id.size} tables---"
    DbClient.log_by :info, "#{@schemas_by_id}" if id_column != @DEFAULT_ID
    DbClient.log_by :info, "---to_do_columns #{@to_do_columns.size} tables by #{id_column}---"
    DbClient.log_by :info, @to_do_columns
  end

  def column_info(table_name, column_name)
    @schemas_by_id[table_name].each do |column|
      return column if column['column_name'] == column_name.to_s
    end
    {}
  end

  def get_tables_id_columns(which_db, id_column)
    @schemas_by_id = {}
    @to_do_columns = {}
    @tables[which_db].each do |table|
      get_table_id_columns(which_db, table['table_name'], id_column)
    end
  end

  # 初始化每单个表ID与外键
  def get_table_id_columns(which_db, table_name, id_column)
    columns_sql = "SELECT column_name,udt_name FROM information_schema.columns WHERE TABLE_NAME='#{table_name}'"
    sql_columns = DbClient.query(which_db, columns_sql).to_a
    if id_column == @DEFAULT_ID
      columns = sql_columns.map {|row| row if row['column_name'] == id_column || row['column_name'].end_with?('_id') }
    else
      columns = sql_columns.map {|row| row if row['column_name'] == id_column }
    end
    @schemas_by_id[table_name] = columns.compact if id_column == 'id_column' || columns.compact.present?
  end

  # 获取单个表关联的外键
  def get_foreign_keys(which_db, table_name, id_column)
    foreign_keys = []
    if table_name == 'app_versions' && id_column == @DEFAULT_ID # Todo custom foreign_keys...
      foreign_key = 'latest_version_id'
    elsif id_column != @DEFAULT_ID
      foreign_key = id_column
    else
      foreign_key = "#{table_name.singularize}_#{id_column}"
    end
    
    @schemas_by_id.each do |tab_name, rows_arr|
      if tab_name != table_name
        rows_arr.each do |row|
          if row['column_name'] == foreign_key
            foreign_keys.push(tab_name => foreign_key)
          end
        end
      end
    end
    DbClient.log_by :info, "foreign_keys for #{table_name} #{id_column}: #{foreign_keys}" if id_column != @DEFAULT_ID
    foreign_keys
  end

  def count_expect_table_by_id(small_table, id_column)
    table_name = small_table['table_name']
    larger_table = table_info(:larger, table_name)

    if larger_table.blank?
      DbClient.log_by :warn, "no table #{table_name} in largerDB." # Todo...
      return
    elsif @schemas_by_id[table_name].blank?
      # DbClient.log_by :warn, "no asso for #{id_column} in table #{table_name}."
      return
    elsif larger_table['rows'] < small_table['rows']
      expect_table = small_table.clone
    else
      expect_table = larger_table.clone
    end
    DbClient.log_by :warn, "To find asso for master_table #{table_name}'#{id_column}." if id_column != @DEFAULT_ID
    
    expect_table['expect_rows'] = larger_table['rows'] + small_table['rows']
    sql_smaller_ids = "SELECT #{id_column} FROM #{table_name}"
    smaller_ids = DbClient.query(:smaller, sql_smaller_ids).to_a.map(){|item| item[id_column]}
    if smaller_ids.size == 0
      @tables[:expect].push(expect_table) and return
    end

    # 计算重复的ID与外键数据
    if small_table['max_id'].to_i == 0
      if id_column == @DEFAULT_ID
        # uuid 为主键, Todo fix repeated uuid
        @tables[:expect].push(expect_table) and return
      end
      # 比如 email 重复了
      ids_str = smaller_ids.map(){|item| "'#{item}'"}.join(',')
      sql_same_ids = "SELECT #{id_column} FROM #{table_name} WHERE #{id_column} in (#{ids_str})"
      same_ids = DbClient.query(:larger, sql_same_ids).to_a.map(){|item| item[id_column]}
      if same_ids.size == 0
        DbClient.log_by :info, "#{table_name} no repeat #{id_column} #{same_ids}, #{small_table}"
      else
        # 重复的数据只保留一份
        expect_table['expect_rows'] -= same_ids.size
        DbClient.log_by :warn, "#{table_name} #{same_ids.size} repeated #{id_column} #{same_ids.take(20)}, #{small_table}"
        foreign_keys = get_foreign_keys(:smaller, table_name, id_column)
        @to_do_columns[table_name] = { conflict_ids: same_ids, foreign_keys: foreign_keys}
      end
    else
      sql_same_ids = "SELECT #{id_column} FROM #{table_name} WHERE #{id_column} in (#{smaller_ids.join(',')})"
      same_ids = DbClient.query(:larger, sql_same_ids).to_a.map(){|item| item[id_column]}.sort
      if same_ids.size == 0
        expect_table['expect_max_id'] = expect_table['max_id']
      else
        if larger_table['rows'] < small_table['rows']
          DbClient.log_by :warn, "table #{table_name} rows #{larger_table['rows']} < #{small_table['rows']}"
          expect_table['expect_max_id'] = expect_table['max_id'] + larger_table['rows'] + @reserved_rows
        else
          expect_table['expect_max_id'] = expect_table['max_id'] + small_table['rows'] + @reserved_rows
        end
      end

      if same_ids.size == 0
        DbClient.log_by :info, "#{table_name} no repeat #{id_column} #{same_ids}, #{small_table}"
      else
        DbClient.log_by :info, "#{table_name} #{same_ids.size} repeated #{id_column} #{same_ids.take(100)}, #{small_table}"
        # 待同步更新的外键数据
        foreign_keys = get_foreign_keys(:smaller, table_name, id_column)
        @to_do_columns[table_name] = { conflict_ids: same_ids, foreign_keys: foreign_keys}
      end
    end
    @tables[:expect].push(expect_table)
  end

  def table_info(which_db, table_name)
    @tables[which_db].each do |table|
      return table if table['table_name'] == table_name.to_s
    end
    []
  end

  def tables_info(which_db, id_column)
    # ["admin_users", "app_versions", "apps", "ar_internal_metadata", "count_pages", "coupon_providers", "coupons", "crontabs", "devices", "edu_emails", "license_codes", "mail_records", "members", "migrations", "orders", "precode_coupons", "products", "schema_migrations", "subscriptions", "sys_configs", "versions"]
    # ["admin_users", "app_versions", "apps", "ar_internal_metadata", "count_pages", "coupon_providers", "coupons", "crontabs", "devices", "edu_emails", "invite_records", "invites", "license_codes", "mail_records", "members", "migrations", "orders", "precode_coupons", "products", "schema_migrations", "subscriptions", "sys_configs", "versions"]
    @tables ||= {}
    return @tables[which_db] if @tables[which_db].present?

    # Todo...不同步的表
    except_tables = %w(admin_users versions ar_internal_metadata edu_emails schema_migrations migrations)
    tables_sql = 'SELECT relname AS table_name,n_live_tup AS rows FROM pg_stat_user_tables ORDER BY n_live_tup DESC'
    tables = DbClient.query(which_db, tables_sql).to_a
    
    tables.each_with_index do |item, index|
      if item['table_name'].in?(except_tables)
        tables[index] = nil
      else
         begin
          result = DbClient.query(which_db, "SELECT max(#{id_column}) AS max_id, count(#{id_column}) AS rows FROM #{item['table_name']}")[0].compact
          tables[index].merge!(result)
          # DbClient.log_by :info, "IntId: #{tables[index]}" # if tables[index]['max_id'].to_i > 0
        rescue ActiveRecord::StatementInvalid => e
          # 主键为 UUID
          # tables[index]['max_id'] = 0
        end
      end
    end
    @tables[which_db] = tables.compact.sort_by {|item| item['table_name']}
  end
  # --- end 检查冲突的ID ---
end
