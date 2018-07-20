# kafka-rtd
Excel RTD server sourcing data from Kafka

![Excel screenshot](doc/ice_video_.gif)


## Installation
1. Clone the repository and go to its folder.
2. Compile the code using Visual Studio, MSBuild or via this handy script file:

   `build.cmd`


3. Register the COM server by running the following script in admin command prompt:
   
   `register.cmd`

## Usage

Once the RTD server has been installed, you can use it from Excel via the RTD macro.
This is the syntax:

`=RTD("kafka",, "HOST","TOPIC")`

`=RTD("kafka",, "HOST","TOPIC", "FIELD")`   // For JSON data