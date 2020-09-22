module DbClient
  LOG_SQL = true

  def logger
    return @logger if @logger.present?
    @logger = Logger.new('log/db_merge.log')
    @logger.formatter = proc do |severity, datetime, progname, msg|
      msg.size < 8 ? "#{msg}" : "\n#{severity.upcase}: #{msg}"
    end
    @logger
  end

  def log_sql(level, msg)
    self.log_by(level, msg) if self::LOG_SQL
  end

  def log_by(level, msg)
    if msg.size < 8
      print msg
    else
      print "\n#{level.upcase}: #{msg}"
    end
    self.logger.send(level, msg)
  end

  def larger(&block)
    DbClient.log_by :info, '---larger---'
    ActiveRecord::Base.connected_to(role: :larger) do
      yield
    end
  end

  def query(which, sql)
    # DbClient.log_by :info, "---query_#{which}---"
    self.send("query_#{which}", sql)
  end

  def query_larger(sql)
    if @larger_conn.blank?
      db_config = Rails.application.config.database_configuration[Rails.env]['larger']
      @larger_conn = ActiveRecord::Base.establish_connection(db_config).connection
      DbClient.log_by :info, "---connect_larger #{db_config['database']}---"
    end

    begin
      @larger_conn.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      if e.inspect.include?('PG::ConnectionBad')
        DbClient.log_by :error, "largerDB: #{e.inspect}"
        @larger_conn.reconnect!
        @larger_conn.execute(sql)
      else
        raise e
      end
    end
  end

  def smaller(&block)
    DbClient.log_by :info, '---smaller---'
    ActiveRecord::Base.connected_to(role: :smaller) do
      yield
    end
  end

  def query_smaller(sql)
    if @smaller_conn.blank?
      db_config = Rails.application.config.database_configuration[Rails.env]['smaller']
      @smaller_conn = ActiveRecord::Base.establish_connection(db_config).connection
      DbClient.log_by :info, "---connect_smaller #{db_config['database']}---"
    end

    begin
      @smaller_conn.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      if e.inspect.include?('PG::ConnectionBad')
        DbClient.log_by :error, "smallerDB: #{e.inspect}"
        @smaller_conn.reconnect!
        @smaller_conn.execute(sql)
      else
        raise e
      end
    end
  end
end

include DbClient
