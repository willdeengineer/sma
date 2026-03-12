import pandas as pd
import numpy as np
import os

np.random.seed(42)

def generate_mock_data(num_rows):
    employee_ids = [f'R{str(i).zfill(6)}' for i in range(1, num_rows + 1)]
    salaries = np.random.randint(2000, 10000, size=num_rows)
    managers = np.random.choice(['Albert', 'Bob', 'Colin', 'Diana', 'Eva'], size=num_rows)
    departments = np.random.choice(['Tester', 'Management', 'Engineering', 'Schoonmaak', 'Sales'], size=num_rows)
    
    df = pd.DataFrame({
        'EMPLOYEE_ID': employee_ids,
        'SALARY': salaries,
        'MANAGER': managers,
        'DEPARTMENT': departments
    })
    return df

row_counts = [10, 1000, 10000, 100000, 1000000]

os.makedirs('./mock_data', exist_ok=True)

for count in row_counts:
    df = generate_mock_data(count)
    filename = f'./mock_data/S_Employee_{count}_rows.csv'
    df.to_csv(filename, index=False)
    print(f'Generated {filename} with {count} rows')