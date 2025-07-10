# pg_gen

A golang code generator to introspect and generate `database/sql` code from Postgres databases

Example usage of the generated code can be found [here](https://github.com/gustapinto/pg_gen/tree/main/example).

## Usage

1. Create a configuration file in your project root. Example:
```json5
{
  // The PostgreSQL database connection string
  "dsn": "postgresql://myuser:mypassword@localhost:5432/mydb?sslmode=disable",

  // The database schemas that will be introspected for code generation
  "schemas": {
    "public": {
      // If views should be included in code generation (Optional, default=false)
      "include_views": true,
      // Tables or views that should be ignored in code generation (Optional, default=null)
      "ignore": [
        "locked_table",
        "super_secret_view"
      ],
      // Golang code generation specific stuff
      "go": {
        // The destination folder
        "dest": "./gen",
        // The generated code package name
        "package": "gen",
        // If the generated entities must include JSON tags (Optional, default=false)
        "emit_json_tags": true
      }
    }
  }
}

```
2. Execute the generator
```bash
go run github.com/gustapinto/pg_gen@latest -config=./example_config.json
```
3. All done! Your generated code is ready to be used

You can also use the generator as a `go:generate` clause. Example:
```go
//go:generate go run github.com/gustapinto/pg_gen@latest -config=./example_config.json
package main

func main() {
    // Your code
}
```