# pg_gen

A golang code generator to introspect and generate `database/sql` code from Postgres databases

## Usage

1. Create a configuration file in your project root. Example:
```json
{
  // The PostgreSQL database connection string
  "dsn": "postgresql://myuser:mypassword@localhost:5432/mydb?sslmode=disable",

  // The database schemas that will be introspected for code generation
  "schemas": {
    "public": {
      // Golang code generation specific stuff
      "go": {
        // The destination folder
        "dest": "./gen",
        // The generated code package name
        "package": "gen",
        // (Optional) If the generated entities must include JSON tags
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