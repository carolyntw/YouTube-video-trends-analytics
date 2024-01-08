# YouTube Video Trends Analytics 

## Introduction
This project demonstrates a data engineering workflow for YouTube analytics using Apache Airflow for automating the Extract, Transform, Load (ETL) and data processing tasks. Airflow is hosted on an AWS EC2 instance, and the processed data is stored in an Amazon S3 bucket. For visualization and analysis, the data is made accessible through Tableau, connecting directly to the S3 bucket.

### Key Features
- **Automated ETL Process**: Utilizes Apache Airflow to automate the data pipeline.
- **Cloud Integration**: Hosted on AWS EC2 for reliable and scalable performance.
- **Data Storage**: Data is stored in an AWS S3 bucket, ensuring secure and scalable storage.
- **Data Visualization**: Integration with Tableau for insightful and interactive data visualizations.

## Tableau Dashboard
![Dashboard Screenshot](images/dashboard.png)
[Link to the Dashboard](https://public.tableau.com/app/profile/kai.yin.chan/viz/GlobalYoutubeVideoTrends/GlobalYoutubeVideoTrends)

### Dashboard Features
- **Global View:** A world map highlighting the view counts and viral scores by country, offering a snapshot of video popularity across the globe.
- **Trending Categories:** A heatmap showcasing the trending video categories within selected countries, enabling users to spot trends quickly.
- **Category Distribution:** A pie chart displaying the view count distribution across different YouTube categories.
- **Popularity by Duration:** A scatter plot correlating video duration with popularity, sized by viral score and colored by total view count.
- **Hourly Video Performance:** A line chart tracking hourly video performance, analyzing view count and viral score dynamics over time.

## Architecture
![Workflow Screenshot](images/workflow.png)

### Technologies Used
- **Apache Airflow**: For orchestrating the ETL pipeline.
- **AWS EC2**: Hosting the Airflow instance.
- **AWS S3**: For data storage.
- **Tableau**: For data visualization.

## Project Execution Workflow

This section details the steps I took to execute the YouTube Analytics Data Engineering project, from starting the Airflow DAGs to visualizing the data with Tableau.

### Prerequisites
- AWS Account with EC2 and S3
- Apache Airflow setup
- Tableau Desktop or Server

### Installation and Setup
1. **Setting up AWS EC2 Instance:** Follow AWS documentation to set up an EC2 instance.
2. **Configuring Apache Airflow:** Install and configure Airflow on the EC2 instance.
3. **S3 Bucket Creation:** Create an S3 bucket for data storage.
4. **Tableau Configuration:** Set up Tableau to connect to the S3 bucket.

### Starting the Airflow DAGs
1. Accessed the Airflow web interface on the AWS EC2 instance.
2. Authenticated and navigated to the DAGs section.
3. Located and triggered the YouTube Analytics DAG to begin processing data.

### Accessing Processed Data in S3
1. Logged into the AWS Management Console and navigated to the S3 section.
2. Located the bucket with the processed data and interacted with it as needed.

### Visualizing Data with Tableau
1. Opened Tableau and connected it to the S3 data source.
2. Imported the datasets and utilized Tableau's suite of tools to create data visualizations.

## Contact Information
If you have any questions or feedback about this project, please get in touch with me at [chankaiy@usc.edu].
