# JSparker 
Created by
- [Massimiliano Ceriani](https://github.com/massicer)
- [Davide Ceresola](https://github.com/dadocere)

## How It works
This application receives as input a `.csv`  file and a `transformation pipeline` written in JSON.
The transformation are applied sequentially in the order they appears in json.
If there is an invalid transformation in json the others are not applied.

## How to start
And the server will start listening on port 8080

## Endpoint

#### Get the status of transformation endpoint

Returns the status 

      method:   GET 
      path:     /jSparkerWebApp/rest/transformation/status
      type:     -


#### Apply transformation Pipeline To Csv file:

Returns the transformation defined in the` pipeline json` applied to a `.csv file` 

      method:   POST 
      path:     /jSparkerWebApp/rest/transformation/new
      type:     form-data


| Attribute   | Description                                                    |
| ----------- | -------------------------------------------------------------- |
| `file`   | your csv file              |
| `pipeline` | your json pipeline     |

#### Export jar
run `mvn package` from the main directory of the project
mvn package uses 'fat jar' to generate the jar

#### Run in jar mode
`spark-submit --class export.JarExecutor --master local[*] --files /Users/davideceresola/git/JSparker/example-data.csv,/Users/davideceresola/git/JSparker/ExamplePipeline.json /Users/davideceresola/git/JSparker/target/jSparkerWebApp-0.0.1-SNAPSHOT.jar /Users/davideceresola/git/JSparker/example-data.csv  /Users/davideceresola/git/JSparker/ExamplePipeline.json`

