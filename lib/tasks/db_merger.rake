namespace :db_merger do
  desc 'Check DB'
  task check_tables: [:environment] do
    @tables = { larger: [], smaller: [], expect: [] }
    count_expect_tables
  end

  def count_expect_tables
    tables_larger = tables_info(:larger)
    tables_smaller = tables_info(:smaller)
    
    tables_smaller.each do |table|
      count_expect_table(table)
    end
    
    # puts "\n---smaller---"
    # puts @tables[:smaller]
    # puts '---larger---'
    # puts @tables[:larger]
    # puts '---expect---'
    puts @tables[:expect]
  end

  def count_expect_table(small_table)
    table_name = small_table['table_name']
    larger_table = table_info(:larger, table_name)
    expect_table = larger_table.clone
    return if larger_table.blank?
    expect_table['expect_rows'] = larger_table['rows'] + small_table['rows']

    if small_table['max_id'].to_i == 0
      # uuid 为主键
    else
      sql_smaller_ids = "SELECT id FROM #{table_name}"
      smaller_ids = DbClient.query(:smaller, sql_smaller_ids).to_a.map(){|item| item['id']}
      sql_same_ids = "SELECT id FROM #{table_name} WHERE id in (#{smaller_ids.join(',')})"
      same_ids = DbClient.query(:larger, sql_same_ids).to_a.map(){|item| item['id']}

      puts "#{same_ids.size} repeated #{same_ids}, #{small_table}"
      expect_table['expect_max_id'] = larger_table['max_id'] + same_ids.size
      
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

    except_tables = %w(admin_users versions ar_internal_metadata edu_emails schema_migrations) # 不同步的表
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
