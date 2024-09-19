#J.S
import pandas as pd

# Load the Excel file into a pandas DataFrame (a table-like structure)
# The file path points to the Excel file on your computer. You may need to adjust this path.
file_path = 'C:/Users/Admin/Desktop/P&R.xlsx'  # Adjust this path if needed
data = pd.read_excel(file_path)  # This reads the Excel file and stores it in the variable 'data'

# Define a function to remove outliers from a specific column using the IQR (Interquartile Range) method
def remove_outliers_iqr(df, column):
    # Calculate the first quartile (Q1), which is the 25th percentile of the data
    Q1 = df[column].quantile(0.25)
    
    # Calculate the third quartile (Q3), which is the 75th percentile of the data
    Q3 = df[column].quantile(0.75)
    
    # Compute the Interquartile Range (IQR) by subtracting Q1 from Q3
    IQR = Q3 - Q1
    
    # Define the lower bound for outliers: anything lower than Q1 - 1.5 * IQR is considered an outlier
    lower_bound = Q1 - 1.5 * IQR
    
    # Define the upper bound for outliers: anything higher than Q3 + 1.5 * IQR is considered an outlier
    upper_bound = Q3 + 1.5 * IQR
    
    # Return a DataFrame with rows where the column values are within the lower and upper bounds (non-outliers)
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

# Call the 'remove_outliers_iqr' function to remove outliers from the 'Production_budget' column
cleaned_data = remove_outliers_iqr(data, 'Production_budget')

# Call the 'remove_outliers_iqr' function again to remove outliers from the 'Opening_Weekend' column
# The cleaned data is now further processed to remove outliers from both columns
cleaned_data = remove_outliers_iqr(cleaned_data, 'Opening_Weekend')

# Save the cleaned data to a new Excel file without including row indices
output_file_path = 'Cleaned_P&R.xlsx'  # Specify the path for saving the cleaned data
cleaned_data.to_excel(output_file_path, index=False)  # Save the DataFrame to the specified Excel file

# Print a confirmation message indicating that the cleaned data has been saved
print(f"Cleaned data saved to {output_file_path}")
