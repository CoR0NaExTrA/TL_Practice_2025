using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

class CsvToSqlImporter
{

    public static void Import( string filePath, string tableName, string connectionString )
    {
        using var connection = new SqlConnection( connectionString );
        connection.Open();

        var columnTypes = GetColumnTypes( connection, tableName );

        using var reader = new StreamReader( filePath );

        var header = reader.ReadLine();
        if ( header == null )
            throw new Exception( "CSV-файл пустой" );

        var columns = header.Split( ';' );

        string insertSql =
            $"INSERT INTO [{tableName}] ({string.Join( ", ", columns.Select( c => $"[{c}]" ) )}) " +
            $"VALUES ({string.Join( ", ", columns.Select( c => "@" + c ) )})";

        string? line;
        while ( ( line = reader.ReadLine() ) != null )
        {
            var values = line.Split( ';' );

            using var command = new SqlCommand( insertSql, connection );
            for ( int i = 0; i < columns.Length; i++ )
            {
                string colName = columns[ i ];
                string rawValue = values[ i ].Trim();

                if ( string.IsNullOrEmpty( rawValue ) || rawValue.ToUpper() == "NULL" )
                {
                    command.Parameters.AddWithValue( "@" + colName, DBNull.Value );
                }
                else
                {
                    if ( columnTypes.TryGetValue( colName, out var sqlType ) )
                    {
                        switch ( sqlType.ToLower() )
                        {
                            case "real":
                            case "float":
                                if ( double.TryParse( rawValue, out var doubleValue ) )
                                    command.Parameters.Add( new SqlParameter( "@" + colName, doubleValue ) );
                                else
                                    command.Parameters.AddWithValue( "@" + colName, DBNull.Value );
                                break;
                            case "int":
                                if ( int.TryParse( rawValue, out var intValue ) )
                                    command.Parameters.Add( new SqlParameter( "@" + colName, intValue ) );
                                else
                                    command.Parameters.AddWithValue( "@" + colName, DBNull.Value );
                                break;
                            case "decimal":
                            case "numeric":
                                if ( decimal.TryParse( rawValue, out var decimalValue ) )
                                    command.Parameters.Add( new SqlParameter( "@" + colName, decimalValue ) );
                                else
                                    command.Parameters.AddWithValue( "@" + colName, DBNull.Value );
                                break;
                            default:
                                command.Parameters.AddWithValue( "@" + colName, rawValue );
                                break;
                        }
                    }
                    else
                    {
                        command.Parameters.AddWithValue( "@" + colName, rawValue );
                    }
                }
            }
            command.ExecuteNonQuery();
        }

        Console.WriteLine( $"Импорт таблицы {tableName} завершён" );
    }

    private static Dictionary<string, string> GetColumnTypes( SqlConnection connection, string tableName )
    {
        var columnTypes = new Dictionary<string, string>();

        string sql = $@"
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{tableName}'";

        using var command = new SqlCommand( sql, connection );
        using var reader = command.ExecuteReader();

        while ( reader.Read() )
        {
            columnTypes[ reader.GetString( 0 ) ] = reader.GetString( 1 );
        }

        return columnTypes;
    }
}

class Program
{
    static void Main()
    {
        string connStr = "Server=localhost\\SQLEXPRESS;Database=landing;Trusted_Connection=True;TrustServerCertificate=True;";
        CsvToSqlImporter.Import( @"D:\Sales\Sales.CustomerTransactions.csv", "CustomerTransaction", connStr );
    }
}
