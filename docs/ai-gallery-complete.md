- title: Binary Classification: Flight delay prediction
- description: In this experiment, we predict whether scheduled passenger flight is delayed or not using a Binary-classifier. Tags: binary classification, Two-Class Logistic regression, Two-Class Boosted Decision Tree, Sweep Parameters, flight delay prediction

[gallery.azure.ai](https://gallery.azure.ai/Experiment/Binary-Classification-Flight-delay-prediction-3 "Binary Classification: Flight delay prediction")

# Binary Classification: Flight delay prediction

In this experiment, we use historical on-time performance and weather data to predict whether the arrival of a scheduled passenger flight will be delayed by more than 15 minutes.

We approach this problem as a classification problem, predicting two classes -- whether the flight will be delayed, or whether it will be on time. Broadly speaking, in machine learning and statistics, classification is the task of identifying the class or category to which a new observation belongs, on the basis of a training set of data containing observations with known categories. Classification is generally a supervised learning problem. Since this is a binary classification task, there are only two classes.

To solve this categorization problem, we will build an experiment using Azure ML Studio. In the experiment, we train a model using a large number of examples from historic flight data, along with an outcome measure that indicates the appropriate category or class for each example. The two classes are labeled 1 if a flight was delayed, and labeled 0 if the flight was on time.

There are five basic steps in building an experiment in Azure ML Studio:

- Create a Model 
  - Step 1: Get Data
  - Step 2: Pre-process Data 
  - Step 3: Define Features
- Train the Model 
  - Step 4: Choose and apply a learning algorithm
- Score and Test the Model
  - Step 5: Predict over new data

## Get the Data

https://www.bts.dot.gov/browse-statistical-products-and-data/bts-publications/airline-service-quality-performance-234-time

### Flight Data
1. Download the Flight Delay Data to your local machine for the period of April to October 2013 from [U.S. Department of Transportation][1].
3. Filter the data to include only the 70 busiest airports in the continental United States.
4. For canceled flights, relabel as delayed by more than 15 mins.
5. Filter out diverted flights.

From the dataset, we selected the following 14 columns: _Year_, _Month_, _DayofMonth_, _DayOfWeek_, _Carrier_, _OriginAirportID_, _DestAirportID_, _CRSDepTime_, _DepDelay_, _DepDel15_, _CRSArrTime_, _ArrDelay_, _ArrDel15_, and _Cancelled_.

These columns contain the following information:  
_Carrier_ \- Code assigned by IATA and commonly used to identify a carrier.  
_OriginAirportID_ \- An identification number assigned by US DOT to identify a unique airport (the flight's origin).  
_DestAirportID_ \- An identification number assigned by US DOT to identify a unique airport (the flight's destination).  
_CRSDepTime_ \- The CRS departure time in local time (hhmm)  
_DepDelay_ \- Difference in minutes between the scheduled and actual departure times. Early departures show negative numbers.  
_DepDel15_ \- A Boolean value indicating whether the departure was delayed by 15 minutes or more (1=Departure was delayed)  
_CRSArrTime_ \- CRS arrival time in local time(hhmm)  
_ArrDelay_ \- Difference in minutes between the scheduled and actual arrival times. Early arrivals show negative numbers.  
_ArrDel15_ \- A Boolean value indicating whether the arrival was delayed by 15 minutes or more (1=Arrival was delayed)  
_Cancelled_ \- A Boolean value indicating whether the arrivalflight was cancelled (1=Flight was cancelled)


### Weather Data
1. Download the Hourly land-based weather observations from NOAA for the same period of April to October 2013 from [NOAA][2].

We also used a set of weather observations: [**Hourly land-based weather observations from NOAA][2].**

The weather data represents observations from airport weather stations, covering the same time period of April-October 2013. Before uploading to Azure ML Studio, we processed the data as follows:

* Weather station IDs were mapped to corresponding airport IDs.
* Weather stations not associated with the 70 busiest airports were filtered out.
* The _Date_ column was split into separate columns: _Year_, _Month_ and _Day_.

From the weather data, the following 26 columns were selected: _AirportID_, _Year_, _Month_, _Day_, _Time_, _TimeZone_, _SkyCondition_, _Visibility_, _WeatherType_, _DryBulbFarenheit_, _DryBulbCelsius_, _WetBulbFarenheit_, _WetBulbCelsius_, _DewPointFarenheit_, _DewPointCelsius_, _RelativeHumidity_, _WindSpeed_, _WindDirection_, _ValueForWindCharacter_, _StationPressure_, _PressureTendency_, _PressureChange_, _SeaLevelPressure_, _RecordType_, _HourlyPrecip_, _Altimeter_

### Airport Codes Dataset

The final dataset used in the experiment contains one row for each U.S. airport, including the airport ID number, airport name, the city, and state (columns: *airport_id*, _city_, _state_, _name_).

##  Pre-process data

A dataset usually requires some pre-processing before it can be analyzed.

![screenshot_of_experiment][3]

**Flight Data Preprocessing**

First, we used the [**Project Columns**][4] module to exclude from the dataset columns that are possible target leakers: _DepDelay_, _DepDel15_, _ArrDelay_, _Cancelled_, _Year_. ![screenshot_of_experiment][5]

The columns _Carrier_, _OriginAirportID_, and _DestAirportID_ represent categorical attributes. However, because they are integers, they are initially parsed as continuous numbers; therefore, we used the [**Metadata Editor**][6] module to convert them to categorical.

![screenshot_of_experiment][7]

We need to join the flight records with the hourly weather records, using the scheduled departure time as one of the join keys. To do this, the _CSRDepTime_ column must be rounded down to the nearest hour using two successive instances of the [**Apply Math Operation**][8] module. ![screenshot_of_experiment][9]

**Weather Data Preprocessing**

Columns that have a large proportion of missing values are excluded using the [**Project Columns**][4] module. These include all string-valued columns: _ValueForWindCharacter_, _WetBulbFarenheit_, _WetBulbCelsius_, _PressureTendency_, _PressureChange_, _SeaLevelPressure_, and _StationPressure_.

![screenshot_of_experiment][10] The [**Clean Missing Data**][11] module is then applied to the remaining columns to remove rows with missing data.

The time of the weather observation is rounded up to the nearest full hour, so that the column can be equi-joined with the scheduled flight departure time. Note that the scheduled flight time and the weather observation times are rounded in opposite directions. This is done to ensure that the model uses only weather observations that happened in the past, relative to flight time. Also note that the weather data is reported in local time, but the origin and destination may be in different time zones. Therefore, an adjustment to time zone difference must be made by subtracting the time zone columns from the scheduled departure time (_CRSDepTime_) and weather observation time (_Time_). These operations are done using the [**Execute R Script**][12] module.

The resulting columns are _Year_, _AdjustedMonth_, _AdjustedDay_, _AirportID_, _AdjustedHour_, _Timezone_, _Visibility_, _DryBulbFarenheit_, _DryBulbCelsius_, _DewPointFarenheit_, _DewPointCelsius_, _RelativeHumidity_, _WindSpeed_, _Altimeter_.

**Joining Datasets**

Flight records are joined with weather data at origin of the flight (_OriginAirportID_) by using the [**Join**][13] module.

![screenshot_of_experiment][14]

Flight records are joined with weather data using the destination of the flight (_DestAirportID_).

![screenshot_of_experiment][15]

**Preparing Training and Validation Samples**

The training and validation samples are created by using the [**Split**][16] module to divide the data into April-September records for training, and October records for validation.

![screenshot_of_experiment][17]

Year and month columns are removed from the training dataset using the [**Project Columns**][4] module. The training data is then separated into equal-height bins using the [**Quantize Data**][18] module, and the same binning method was applied to the validation data. ![screenshot_of_experiment][19]

The training data is split once more, into a training dataset and an optional validation dataset.

![screenshot_of_experiment][20]

##  Define Features

In machine learning, _features_ are individual measurable properties of something you're interested in. Finding a good set of features for creating a predictive model requires experimentation and knowledge about the problem at hand. Some features are better for predicting the target than others. Also, some features have a strong correlation with other features, so they will not add much new information to the model and can be removed. In order to build a model, we can use all the features available, or we can select a subset of the features in the dataset. Typically you can try selecting different features, and then running the experiment again, to see if you get better results.

The various features are the weather conditions at the arrival and destination airports, departure and arrival times, the airline carrier, the day of month, and the day of the week.

##  Choose and apply a learning algorithm.

**Model Training and Validation**

We created a model using the [**Two-Class Boosted Decision Tree**][21] module and trained it using the training dataset. To determine the optimal parameters, we connected the output port of **Two-Class Boosted Decision Tree** to the [**Sweep Parameters**][22] module.

![screenshot_of_experiment][23]

The model is optimized for the best AUC using 10-fold random parameter sweep.

![screenshot_of_experiment][24]

For comparison, we created a model using the [**Two-Class Logistic Regression**][25] module, and optimized it in the same manner.

The result of the experiment is a trained classification model that can be used to score new samples to make predictions. We used the validation set to generate scores from the trained models, and then used the [**Evaluate Model**][26] module to analyze and compare the quality of the models.

## Predict Using New Data

Now that we've trained the model, we can use it to score the other part of our data (the last month (October) records that were set aside for validation) and to see how well our model predicts and classifies new data.

Add the [**Score Model**][27] module to the experiment canvas, and connect the left input port to the output of the **Train Model** module. Connect the right input port to the validation data (right port) of the **[Split**][16] module.

After you run the experiment, you can view the output from the **Score Model** module by clicking the output port and selecting **Visualize**. The output includes the scored labels and the probabilities for the labels.

Finally, to test the quality of the results, add the **[Evaluate Model**][26] module to the experiment canvas, and connect the left input port to the output of the **Score Model** module. Note that there are two input ports for **Evaluate Model**, because the module can be used to compare two models. In this experiment, we compare the performance of the two different algorithms: the one created using **Two-Class Boosted Decision Tree** and the one created using **Two-Class Logistic Regression**. Run the experiment and view the output of the **Evaluate Model** module, by clicking the output port and selecting **Visualize**.

## Results

The boosted decision tree model has AUC of 0.697 on the validation set, which is slightly better than the logistic regression model, with AUC of 0.675. ![screenshot_of_experiment][28]

**Post-Processing**

To make the results easier to analyze, we used the _airportID_ field to join the dataset that contains the airport names and locations.

# Binary Classification: Flight delay prediction

In this experiment, we use historical on-time performance and weather data to predict whether the arrival of a scheduled passenger flight will be delayed by more than 15 minutes.

We approach this problem as a classification problem, predicting two classes -- whether the flight will be delayed, or whether it will be on time. Broadly speaking, in machine learning and statistics, classification is the task of identifying the class or category to which a new observation belongs, on the basis of a training set of data containing observations with known categories. Classification is generally a supervised learning problem. Since this is a binary classification task, there are only two classes.

To solve this categorization problem, we will build an experiment using Azure ML Studio. In the experiment, we train a model using a large number of examples from historic flight data, along with an outcome measure that indicates the appropriate category or class for each example. The two classes are labeled 1 if a flight was delayed, and labeled 0 if the flight was on time.

There are five basic steps in building an experiment in Azure ML Studio:

Create a Model

Train the Model

Score and Test the Model

* * *

## Data

**Passenger flight on-time performance data taken from TranStats data collection from [U.S. Department of Transportation][1].**

The dataset contains flight delay data for the period April-October 2013. Before uploading the data to Azure ML Studio, we pre-processed it as follows:

* Filtered to include only the 70 busiest airports in the continental United States.
* For canceled flights, relabeled as delayed by more than 15 mins.
* Filtered out diverted flights.

From the dataset, we selected the following 14 columns: _Year_, _Month_, _DayofMonth_, _DayOfWeek_, _Carrier_, _OriginAirportID_, _DestAirportID_, _CRSDepTime_, _DepDelay_, _DepDel15_, _CRSArrTime_, _ArrDelay_, _ArrDel15_, and _Cancelled_.

These columns contain the following information:  
_Carrier_ \- Code assigned by IATA and commonly used to identify a carrier.  
_OriginAirportID_ \- An identification number assigned by US DOT to identify a unique airport (the flight's origin).  
_DestAirportID_ \- An identification number assigned by US DOT to identify a unique airport (the flight's destination).  
_CRSDepTime_ \- The CRS departure time in local time (hhmm)  
_DepDelay_ \- Difference in minutes between the scheduled and actual departure times. Early departures show negative numbers.  
_DepDel15_ \- A Boolean value indicating whether the departure was delayed by 15 minutes or more (1=Departure was delayed)  
_CRSArrTime_ \- CRS arrival time in local time(hhmm)  
_ArrDelay_ \- Difference in minutes between the scheduled and actual arrival times. Early arrivals show negative numbers.  
_ArrDel15_ \- A Boolean value indicating whether the arrival was delayed by 15 minutes or more (1=Arrival was delayed)  
_Cancelled_ \- A Boolean value indicating whether the arrivalflight was cancelled (1=Flight was cancelled)

We also used a set of weather observations: **[Hourly land-based weather observations from NOAA][2].**

The weather data represents observations from airport weather stations, covering the same time period of April-October 2013. Before uploading to Azure ML Studio, we processed the data as follows:

* Weather station IDs were mapped to corresponding airport IDs.
* Weather stations not associated with the 70 busiest airports were filtered out.
* The _Date_ column was split into separate columns: _Year_, _Month_ and _Day_.

From the weather data, the following 26 columns were selected: _AirportID_, _Year_, _Month_, _Day_, _Time_, _TimeZone_, _SkyCondition_, _Visibility_, _WeatherType_, _DryBulbFarenheit_, _DryBulbCelsius_, _WetBulbFarenheit_, _WetBulbCelsius_, _DewPointFarenheit_, _DewPointCelsius_, _RelativeHumidity_, _WindSpeed_, _WindDirection_, _ValueForWindCharacter_, _StationPressure_, _PressureTendency_, _PressureChange_, _SeaLevelPressure_, _RecordType_, _HourlyPrecip_, _Altimeter_

**Airport Codes Dataset**

The final dataset used in the experiment contains one row for each U.S. airport, including the airport ID number, airport name, the city, and state (columns: *airport_id*, _city_, _state_, _name_).

##  Pre-process data

A dataset usually requires some pre-processing before it can be analyzed.

![screenshot_of_experiment][3]

**Flight Data Preprocessing**

First, we used the **[Project Columns**][4] module to exclude from the dataset columns that are possible target leakers: _DepDelay_, _DepDel15_, _ArrDelay_, _Cancelled_, _Year_. ![screenshot_of_experiment][5]

The columns _Carrier_, _OriginAirportID_, and _DestAirportID_ represent categorical attributes. However, because they are integers, they are initially parsed as continuous numbers; therefore, we used the **[Metadata Editor**][6] module to convert them to categorical.

![screenshot_of_experiment][7]

We need to join the flight records with the hourly weather records, using the scheduled departure time as one of the join keys. To do this, the _CSRDepTime_ column must be rounded down to the nearest hour using two successive instances of the **[Apply Math Operation**][8] module. ![screenshot_of_experiment][9]

**Weather Data Preprocessing**

Columns that have a large proportion of missing values are excluded using the **[Project Columns**][4] module. These include all string-valued columns: _ValueForWindCharacter_, _WetBulbFarenheit_, _WetBulbCelsius_, _PressureTendency_, _PressureChange_, _SeaLevelPressure_, and _StationPressure_.

![screenshot_of_experiment][10] The **[Clean Missing Data**][11] module is then applied to the remaining columns to remove rows with missing data.

The time of the weather observation is rounded up to the nearest full hour, so that the column can be equi-joined with the scheduled flight departure time. Note that the scheduled flight time and the weather observation times are rounded in opposite directions. This is done to ensure that the model uses only weather observations that happened in the past, relative to flight time. Also note that the weather data is reported in local time, but the origin and destination may be in different time zones. Therefore, an adjustment to time zone difference must be made by subtracting the time zone columns from the scheduled departure time (_CRSDepTime_) and weather observation time (_Time_). These operations are done using the **[Execute R Script**][12] module.

The resulting columns are _Year_, _AdjustedMonth_, _AdjustedDay_, _AirportID_, _AdjustedHour_, _Timezone_, _Visibility_, _DryBulbFarenheit_, _DryBulbCelsius_, _DewPointFarenheit_, _DewPointCelsius_, _RelativeHumidity_, _WindSpeed_, _Altimeter_.

**Joining Datasets**

Flight records are joined with weather data at origin of the flight (_OriginAirportID_) by using the **[Join**][13] module.

![screenshot_of_experiment][14]

Flight records are joined with weather data using the destination of the flight (_DestAirportID_).

![screenshot_of_experiment][15]

**Preparing Training and Validation Samples**

The training and validation samples are created by using the **[Split**][16] module to divide the data into April-September records for training, and October records for validation.

![screenshot_of_experiment][17]

Year and month columns are removed from the training dataset using the **[Project Columns**][4] module. The training data is then separated into equal-height bins using the **[Quantize Data**][18] module, and the same binning method was applied to the validation data. ![screenshot_of_experiment][19]

The training data is split once more, into a training dataset and an optional validation dataset.

![screenshot_of_experiment][20]

##  Define Features

In machine learning, _features_ are individual measurable properties of something you're interested in. Finding a good set of features for creating a predictive model requires experimentation and knowledge about the problem at hand. Some features are better for predicting the target than others. Also, some features have a strong correlation with other features, so they will not add much new information to the model and can be removed. In order to build a model, we can use all the features available, or we can select a subset of the features in the dataset. Typically you can try selecting different features, and then running the experiment again, to see if you get better results.

The various features are the weather conditions at the arrival and destination airports, departure and arrival times, the airline carrier, the day of month, and the day of the week.

##  Choose and apply a learning algorithm.

**Model Training and Validation**

We created a model using the **[Two-Class Boosted Decision Tree**][21] module and trained it using the training dataset. To determine the optimal parameters, we connected the output port of **Two-Class Boosted Decision Tree** to the **[Sweep Parameters**][22] module.

![screenshot_of_experiment][23]

The model is optimized for the best AUC using 10-fold random parameter sweep.

![screenshot_of_experiment][24]

For comparison, we created a model using the **[Two-Class Logistic Regression**][25] module, and optimized it in the same manner.

The result of the experiment is a trained classification model that can be used to score new samples to make predictions. We used the validation set to generate scores from the trained models, and then used the **[Evaluate Model**][26] module to analyze and compare the quality of the models.

## Predict Using New Data

Now that we've trained the model, we can use it to score the other part of our data (the last month (October) records that were set aside for validation) and to see how well our model predicts and classifies new data.

Add the **[Score Model**][27] module to the experiment canvas, and connect the left input port to the output of the **Train Model** module. Connect the right input port to the validation data (right port) of the **[Split**][16] module.

After you run the experiment, you can view the output from the **Score Model** module by clicking the output port and selecting **Visualize**. The output includes the scored labels and the probabilities for the labels.

Finally, to test the quality of the results, add the **[Evaluate Model**][26] module to the experiment canvas, and connect the left input port to the output of the **Score Model** module. Note that there are two input ports for **Evaluate Model**, because the module can be used to compare two models. In this experiment, we compare the performance of the two different algorithms: the one created using **Two-Class Boosted Decision Tree** and the one created using **Two-Class Logistic Regression**. Run the experiment and view the output of the **Evaluate Model** module, by clicking the output port and selecting **Visualize**.

## Results

The boosted decision tree model has AUC of 0.697 on the validation set, which is slightly better than the logistic regression model, with AUC of 0.675. ![screenshot_of_experiment][28]

**Post-Processing**

To make the results easier to analyze, we used the _airportID_ field to join the dataset that contains the airport names and locations.

[1]: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
[2]: http://cdo.ncdc.noaa.gov/qclcd_ascii/
[3]: https://az712634.vo.msecnd.net/samplesimg/v2/4/flight21.PNG ""
[4]: https://msdn.microsoft.com/library/azure/1ec722fa-b623-4e26-a44e-a50c6d726223
[5]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight1.PNG ""
[6]: https://msdn.microsoft.com/library/azure/370b6676-c11c-486f-bf73-35349f842a66
[7]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight2.PNG ""
[8]: https://msdn.microsoft.com/library/azure/6bd12c13-d9c3-4522-94d3-4aa44513af57
[9]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight6.PNG ""
[10]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight7.PNG ""
[11]: https://msdn.microsoft.com/library/azure/d2c5ca2f-7323-41a3-9b7e-da917c99f0c4
[12]: https://msdn.microsoft.com/en-us/library/azure/dn905952.aspx
[13]: https://msdn.microsoft.com/library/azure/124865f7-e901-4656-adac-f4cb08248099
[14]: https://az712634.vo.msecnd.net/samplesimg/v2/4/flight11.PNG ""
[15]: https://az712634.vo.msecnd.net/samplesimg/v2/4/flight14.PNG ""
[16]: https://msdn.microsoft.com/library/azure/70530644-c97a-4ab6-85f7-88bf30a8be5f
[17]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight15.PNG ""
[18]: https://msdn.microsoft.com/library/azure/61dd433a-ee80-4ac3-87f0-b54708644d93
[19]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight16.PNG ""
[20]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight17.PNG ""
[21]: https://msdn.microsoft.com/library/azure/e3c522f8-53d9-4829-8ea4-5c6a6b75330c
[22]: https://msdn.microsoft.com/library/azure/038d91b6-c2f2-42a1-9215-1f2c20ed1b40
[23]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight18b.PNG ""
[24]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight19.PNG ""
[25]: https://msdn.microsoft.com/library/azure/b0fd7660-eeed-43c5-9487-20d9cc79ed5d
[26]: https://msdn.microsoft.com/library/azure/927d65ac-3b50-4694-9903-20f6c1672089
[27]: https://msdn.microsoft.com/library/azure/401b4f92-e724-4d5a-be81-d5b0ff9bdb33
[28]: https://az712634.vo.msecnd.net/samplesimg/v1/4/flight20.PNG ""

