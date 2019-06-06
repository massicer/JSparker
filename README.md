# JSparker 

[TOCM]

[TOC]

Created by
- [Massimiliano Ceriani](https://github.com/massicer)
- [Davide Ceresola](https://github.com/dadocere)

## How It works
This application receives as input a `.csv`  file and a `transformation pipeline` written in JSON.
The transformation are applied sequentially in the order they appears in json.
If there is an invalid transformation in json the others are not applied.
The transformation pipeline are generated from `grafterizer` tool.

## Server Mode
Generate and deploy a `war` file

### Endpoint

##### Get the status of transformation endpoint

Returns the status 

      method:   GET 
      path:     /jSparkerWebApp/rest/transformation/status
      type:     -


##### Apply transformation Pipeline To Csv file:

Returns the transformation defined in the` pipeline json` applied to a `.csv file` 

      method:   POST 
      path:     /jSparkerWebApp/rest/transformation/new
      type:     form-data


| Attribute   | Description                                                    |
| ----------- | -------------------------------------------------------------- |
| `file`   | your csv file              |
| `pipeline` | your json pipeline     |

### Standalone Mode (Jar)

#### How to Export jar
run `mvn package` from the main directory of the project
[mvn package uses 'fat jar' to generate the jar.](https://crunchify.com/how-to-create-build-java-project-including-all-dependencies-using-maven-maven-resources-maven-dependency-maven-jar-plugin-tutorial/)

#### Run generated Jar
- The jar execution mode takes two parameters
- the `csv file`
- the `json pipeline`generated from grafterizer
- The file paths must be absolutely path
- Type this command to run the jar `spark-submit --class export.JarExecutor --master local[*]  JsparkerExecutable.jar example-data.csv ExamplePipeline.json`
- to run in different cluster mode `spark-submit  --class export.JarExecutor  --master spark://127.0.0.1:6066  JsparkerExecutable.jar example-data.csv ExamplePipeline.json`

