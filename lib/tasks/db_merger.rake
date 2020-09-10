namespace :db_merger do
  desc 'Check DB and update Conflict IDs'
  task check_tables: [:environment] do
    @apply_fix = false
    start_time = Time.now.to_f.round(3)
    puts "Start from #{start_time}."

    @tables = { larger: [], smaller: [], expect: [] }
    count_expect_tables
    update_all_tables_conflict(:smaller)
    # update_conflict_id(:smaller, 'app_versions', [{"apps"=>"latest_version_id"}])

    end_time = Time.now.to_f.round(3)
    time_spent = (end_time - start_time).round(3)
    puts "\nTotal check spent #{time_spent} Seconds, end at #{end_time}."
  end

  def update_all_tables_conflict(which_db)
    num = 1
    @to_do_columns.each do |table_name, asso|
      # 按冲突ID更新主表、以及关联表
      conflict_ids = @to_do_columns[table_name][:conflict_ids]
      puts "\n---smaller table#{num} #{table_name}, #{conflict_ids.size} conflict_ids, #{asso[:foreign_keys].size} foreign_keys: #{asso[:foreign_keys]}"
      conflict_ids.each_with_index do |old_id, index|
        update_conflict_id(which_db, table_name, old_id, index, asso[:foreign_keys])
      end
      num += 1
    end
  end

  # table_name 主表名，asso_tables 关联表的表名，与外键名
  def update_conflict_id(which_db, table_name, old_id, index, asso_tables = [])
    curr_new_id = table_info(:larger, table_name)['max_id'] + index + 2
    larger_table = table_info(:larger, table_name)

    ActiveRecord::Base.transaction do
      asso_tables.each_with_index do |table, asso_index|
        # 单个关联表
        asso_table = table.keys.first
        foreign_key = table.values.first
        
        if @apply_fix
          asso_sql = "UPDATE #{asso_table} SET #{foreign_key}=#{curr_new_id} WHERE #{foreign_key}=#{old_id}"
        else
          asso_sql = "SELECT id FROM #{asso_table} WHERE #{foreign_key}=#{old_id}"
        end
        asso_result = DbClient.query(which_db, asso_sql)
        asso_rows = asso_result.cmd_tuples

        if index < 2
          puts "ASSO#{asso_index} SQL#{index}: #{asso_table} update #{asso_rows} rows from ID#{old_id} to #{curr_new_id}, largerMaxId #{larger_table['max_id']}, #{asso_sql}"
        else
          print "A#{old_id}."
        end
      end

      # 同步修改主表ID
      if @apply_fix
        main_sql = "UPDATE #{table_name} SET id=#{curr_new_id} WHERE id=#{old_id}"
      else
        main_sql = "SELECT id FROM #{table_name} WHERE id=#{old_id}"
      end
      main_result = DbClient.query(which_db, main_sql)
      main_rows = main_result.cmd_tuples

      expect_main_rows = 1# @to_do_columns[table_name][:conflict_ids].size
      if main_rows != expect_main_rows
        error = "MAIN SQL#{index}: #{table_name} expect #{expect_main_rows} rows, but #{main_rows} rows"
        puts "Error #{error}"
        # debugger
        raise error
      end

      if index < 2
        puts "MAIN SQL#{index}: #{table_name} update #{main_rows} rows from ID#{old_id} to #{curr_new_id}, largerMaxId #{larger_table['max_id']}, #{main_sql}"
        puts '' if index == 0
      else
        print "M#{old_id}."
      end
    end
  end

  # 以下用于合并前的数据检查，与信息记录
  def count_expect_tables
    tables_larger = tables_info(:larger)
    tables_smaller = tables_info(:smaller)
    get_tables_id_columns(:smaller)
    tables_smaller.each do |table|
      count_expect_table(table)
    end
    
    # puts "\n---smaller #{@tables[:smaller].size} tables---"
    # puts @tables[:smaller]
    # puts "---larger #{@tables[:larger].size} tables---"
    # puts @tables[:larger]
    puts "\n---expect #{@tables[:expect].size} tables---"
    puts @tables[:expect]
    puts "\n---schemas #{@schemas.size} tables---"
    # puts "#{@schemas}"
    puts "\n---to_do_columns #{@to_do_columns.size} tables by ID---"
    puts @to_do_columns
  end

  def get_tables_id_columns(which_db)
    @schemas = {}
    @to_do_columns = {}
    @tables[which_db].each do |table|
      get_table_id_columns(which_db, table['table_name'])
    end
  end

  # 初始化每单个表ID与外键
  def get_table_id_columns(which_db, table_name)
    columns_sql = "SELECT column_name,udt_name FROM information_schema.columns WHERE TABLE_NAME='#{table_name}'"
    columns = DbClient.query(which_db, columns_sql).to_a.map {|row| row if row['column_name'] == 'id' || row['column_name'].end_with?('_id') }.compact
    @schemas[table_name] = columns
  end

  # 获取单个表关联的外键
  def get_foreign_keys(which_db, table_name, id_column = 'id')
    foreign_keys = []
    if table_name == 'app_versions' && id_column == 'id' # Todo...
      foreign_key = 'latest_version_id'
    else
      foreign_key = "#{table_name.singularize}_#{id_column}"
    end
    
    @schemas.each do |tab_name, rows_arr|
      if tab_name != table_name
        rows_arr.each do |row|
          if row['column_name'] == foreign_key
            foreign_keys.push(tab_name => foreign_key)
          end
        end
      end
    end
    # puts "foreign_keys for #{table_name} #{id_column}: #{foreign_keys}"
    foreign_keys
  end

  def count_expect_table(small_table)
    table_name = small_table['table_name']
    larger_table = table_info(:larger, table_name)
    expect_table = larger_table.clone
    if larger_table.blank?
      puts "Warn no table #{table_name} in largerDB." # Todo...
      return
    end
    
    expect_table['expect_rows'] = larger_table['rows'] + small_table['rows']
    sql_smaller_ids = "SELECT id FROM #{table_name}"
    smaller_ids = DbClient.query(:smaller, sql_smaller_ids).to_a.map(){|item| item['id']}
    if smaller_ids.size == 0
      @tables[:expect].push(expect_table)
      return 
    end

    # 计算重复的ID与外键数据
    if small_table['max_id'].to_i == 0
      # uuid 为主键
      # ids_str = smaller_ids.map(){|id| "'#{id}'"}.join(',')
      # sql_same_ids = "SELECT id FROM #{table_name} WHERE id in (#{ids_str})"
      # same_ids = DbClient.query(:larger, sql_same_ids).to_a.map(){|item| item['id']}
      # if same_ids.size == 0
      #   puts "#{table_name} no repeat uuid #{same_ids}, #{small_table}"
      # else
      #   puts "#{table_name} #{same_ids.size} repeated uuid #{same_ids}, #{small_table}"
      #   foreign_keys = get_foreign_keys(:smaller, table_name, 'id')
      # end
    else
      sql_same_ids = "SELECT id FROM #{table_name} WHERE id in (#{smaller_ids.join(',')})"
      same_ids = DbClient.query(:larger, sql_same_ids).to_a.map(){|item| item['id']}.sort
      expect_table['expect_max_id'] = larger_table['max_id'] + same_ids.size + 1

      if same_ids.size == 0
        puts "#{table_name} no repeat id #{same_ids}, #{small_table}"
      else
        puts "#{table_name} #{same_ids.size} repeated id #{same_ids}, #{small_table}"
        # 待同步更新的外键数据
        foreign_keys = get_foreign_keys(:smaller, table_name, 'id')
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

  def tables_info(which_db)
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
          result = DbClient.query(which_db, "SELECT max(id) AS max_id FROM #{item['table_name']}")[0].compact
          tables[index].merge!(result)
          # puts "IntId: #{tables[index]}" # if tables[index]['max_id'].to_i > 0
        rescue ActiveRecord::StatementInvalid => e
          # 主键为 UUID
          # tables[index]['max_id'] = 0
        end
      end
    end
    @tables[which_db] = tables.compact.sort_by {|item| item['table_name']}
  end
end
