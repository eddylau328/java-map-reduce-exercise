# java-map-reduce-exercise

## Setup for development

1. follow the guides to [setup Java](https://code.visualstudio.com/docs/java/java-project) in vscode
2. install the dependencies by maven
3. decompress the input directory by `unzip ./docs/input.zip -d .`

# Test in jar

1. Export Jar (in vscode, `cmd+shift+p` + Type `> Java: Export Jar`, then select App and all dependencies)
2. After that you should have a `java-map-reduce-exercise.jar` in the project root directory
3. run `hadoop jar java-map-reduce-exercise.jar ./input ./output/test` in the project root directory

- make sure you install hadoop
- you can replace any directory for the output results
