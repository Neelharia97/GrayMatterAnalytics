# Gray Matter Analytics
`yea`
### Objective: 

Create a scalable, easily maintainable solution that does the following: 

      ·       Ingests the Azure dataset (NYC Taxi and Limousine yellow dataset - Azure Open Datasets | Microsoft Docs) 
      ·       Summarizes  
                o Mean/medium cost, prices, and passenger counts. 
                o Aggregated by payment type, year, month.       
      ·       Results are outputted in CSV or parquet format.       
      ·       Include source code and a ReadMe File with information on how to run your script and any other dependencies. 
      ·       Feel free to use whichever scripting language you prefer to complete the assessment.  

## Overview

        The project is divided into two parts. 
        1. Data-Prep
           This section is responsible for generating datasets within the present working directory, the code looks at the data present in the Azure blob containers 
           and filters it according to month and year based on start and end dates. 
           Once all the parquet files to be downloaded are analyzed, they are pulled into the present working directory and inserted into a spark data frame by the summarize code block. 
          
        2. Analyze/Summarize 
          This section consists of many subsections, as below: 
          a. Transformations:
             Handled under the `transformation` function of the class.
             It is solely responsible for handling columns of interest, mapping IDs to their actual values (paymentIds, rateCodeIds) 
             It also filters the transactions according to start and end date.
             The data is divided into three subsections:
                 1. `Total`: Consists of all transactions including Unknown, Voided Trips & Disputes
                 2. `Valid Trips`: Consists of all trips that have completed payment transactions i.e. Payment Types in Cash & Card (Feel free to define valid trips)
                 3. `Invalid Trips`: Incomplete payment transactions i.e. Payment Types in Dispute and voided trips
            Analysis is provided for all types of data.
          
          b. Primary Analysis:
            It handles the analysis in the scope of the assessment, referenced in the objective.

          c. Secondary Analysis:
            It handles the computation of other parameters, such as tolls paid, tips collected, and revenue generated aggregated by PaymentType, month, year & rateCode.
            This helps in understanding the most used payment method along with what rate type is applied in the majority.

          d. Generate Report
            The `generate_report` function takes care of generating CSV files in the present working directory.
            It creates specific folders for all types of subsections discussed above for both primary and secondary analysis.

          e. Clean-Up
            The `cleanUp` function takes care of cleaning all the .parquet files generated in the PWD while data-prep after generating the results.
          
            

## Usage
      There a multiple ways to use this as per preference. For easier use, I would prefer using the Python notebook on Google Colab to support dependencies.
        1. Google Colab:
           Input start_date and end_date when prompted and execute the main code block. 
        
        2. Running in Local
          Running locally is a bit complicated due to PySparks dependency on Java, however, if you have PySpark setup you can use the following two commands:
          `pip install -r requirements.txt`
          `python main.py <start_date> <end_date>`


