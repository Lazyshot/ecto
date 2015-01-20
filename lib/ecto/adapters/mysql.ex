defmodule Ecto.Adapters.MySQL do
  @moduledoc """
  Adapter module for MySQL

  It handles and pools the connections to the MySQL
  database using `liveforeverx/mariaex` with `poolboy`.

  ## Options

  The options should be given via `Ecto.Repo.conf/0`.

  `:hostname` - Server hostname;
  `:port` - Server port (default: 5432);
  `:username` - Username;
  `:password` - User password;
  `:size` - The number of connections to keep in the pool;
  `:max_overflow` - The maximum overflow of connections (see poolboy docs);
  `:parameters` - Keyword list of connection parameters;
  `:ssl` - Set to true if ssl should be used (default: false);
  `:ssl_opts` - A list of ssl options, see ssl docs;
  `:lazy` - If false all connections will be started immediately on Repo startup (default: true)

  """

  use Ecto.Adapters.SQL, :mariaex
  @behaviour Ecto.Adapter.Storage

  ## Storage API

  @doc false
  def storage_up(opts) do
    database   = Keyword.fetch!(opts, :database)
    char_set   = Keyword.get(opts, :char_set, "utf8")
    collation  = Keyword.get(opts, :collation, "utf8_unicode_ci")

    output =
      run_with_mysql opts,
        "CREATE DATABASE " <> database <> " " <> 
        "DEFAULT CHARACTER SET = #{char_set} " <>
        "DEFAULT COLLATE = #{collation}"

    cond do
      String.length(output) == 0                 -> :ok
      String.contains?(output, "already exists") -> {:error, :already_up}
      true                                       -> {:error, output}
    end
  end


  @doc false
  def storage_down(opts) do
    output = run_with_mysql(opts, "DROP DATABASE #{opts[:database]}")

    cond do
      String.length(output) == 0                 -> :ok
      String.contains?(output, "does not exist") -> {:error, :already_down}
      true                                       -> {:error, output}
    end
  end

  defp run_with_mysql(database, sql_command) do
    command = ""

    if password = database[:password] do
      command = ~s(MYSQL_PWD=#{password} )
    end

    if port = database[:port] do
      command = ~s(MYSQL_TCP_PORT=#{port} ) <> command
    end

    command =
      command <>
      ~s(mysql --silent ) <>
      ~s(-u #{database[:username]} ) <>
      ~s(-h #{database[:hostname]} ) <>
      ~s(-e "#{sql_command};" )

    String.to_char_list(command)
    |> :os.cmd
    |> List.to_string
  end
end
