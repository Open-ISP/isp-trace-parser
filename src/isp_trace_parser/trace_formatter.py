from datetime import timedelta


import pandas as pd


def trace_formater(trace_data: pd.DataFrame):
    trace_data = trace_data.melt(id_vars=['Year', 'Month', 'Day'],
                                 value_vars=[f'{i:02d}' for i in range(1, 49)],
                                 var_name='time_label', value_name='Data')
    """Takes trace data in the AEMO format and converts it to Datetime column and data column format.
    
    AEMO provides ISP trace data in a format where there are separates columns specifying the year, month, and day, 
    and then a different data column for each half hour in the day (labeled 01, 02 . . . 48). This function converts 
    that data format into one where a single column specifies the datatime of data point, with the datetime 
    specifying the end of the sample period, and another column specifies the data point value.
    
    Examples
    
    In this example we just specify three of the half your columns for brevity.
    
    >>> aemo_format_data = pd.Dataframe({
    ... 'Year': [2024, 2024],
    ... 'Month': [6, 6],
    ... 'Day':[1, 2],
    ... '01': [11.2, 15.3],
    ... '02': [30.7, 20.4],
    ... '48': [17.1, 18.9]
    
    >>> aemo_format_data
    
    >>> trace_formatter(aemo_format_data)
    
    Args: 
        trace_data: DataFrame with columns 'Hour', 'Month', 'Year', and '01', '02', . . . '48'. The hour, month and year 
            columns specify the day which the data pertains to, and the 01, 02 . . . 48 contain data for each half 
            for each half hour of the day, starting with the time period ending 00:30:00 and ending with the time period 
            ending 00:00:00.
            
    Returns: DataFrame with a column 'Datetime' specifying the end of the sample period, ending on the half hour, and a 
        column specifying the data for each sample period.
    """

    def get_hour(time_label):
        return timedelta(hours=int(time_label) // 2)

    def get_minute(time_label):
        return timedelta(minutes=int(time_label) % 2 * 30)

    trace_data['Hour'] = trace_data['time_label'].apply(lambda x: get_hour(x))

    trace_data['Minute'] = trace_data['time_label'].apply(lambda x: get_minute(x))

    trace_data['Datetime'] = pd.to_datetime(trace_data['Year'].astype(str).str.zfill(2) + '-' +
                                            trace_data['Month'].astype(str).str.zfill(2) + '-' +
                                            trace_data['Day'].astype(str).str.zfill(2) + ' ' + '00:00:00')

    trace_data['Datetime'] = trace_data['Datetime'] + trace_data['Hour'] + trace_data['Minute']

    trace_data = trace_data[['Datetime', 'Data']].sort_values('Datetime').reset_index(drop=True)

    return trace_data
