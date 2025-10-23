import click
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# SQL connection details
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USER')};"
    f"PWD={os.getenv('SQL_PASSWORD')}"
)


@click.group()
def cli():
    """SQL ETL CLI Tool"""
    pass

# ------------------ Extract ------------------
@cli.command()
@click.option('--table', required=True, help='SQL table to extract from')
def extract(table):
    """Extract data from SQL Server"""
    click.echo(f"🔍 Extracting data from SQL table: {table}")
    conn = pyodbc.connect(conn_str)
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv('extracted_sql.csv', index=False)
    click.echo(f"✅ Extracted {len(df)} rows")
    click.echo("📁 Saved to extracted_sql.csv")

# ------------------ Transform ------------------
@cli.command()
@click.option('--input', default='extracted_sql.csv', help='Input file to transform')
@click.option('--drop-null', is_flag=True, help='Drop rows with null values')
@click.option('--drop-cols', multiple=True, help='Columns to drop')
@click.option('--filter-dept', help='Filter by department name')
def transform(input, drop_null, drop_cols, filter_dept):
    """Transform SQL data"""
    click.echo(f"🔧 Transforming {input}")
    df = pd.read_csv(input)

    if drop_cols:
        df = df.drop(columns=list(drop_cols), errors='ignore')
        click.echo(f"🧹 Dropped columns: {', '.join(drop_cols)}")

    if drop_null:
        df = df.dropna()
        click.echo("🧹 Dropped nulls")

    if filter_dept:
        df = df[df['department'] == filter_dept]
        click.echo(f"🔎 Filtered by department: {filter_dept}")

    df.to_csv('transformed_sql.csv', index=False)
    click.echo("📁 Saved to transformed_sql.csv")

# ------------------ Load ------------------
@cli.command()
@click.option('--target', default='loaded_sql.csv', help='Target file to save')
def load(target):
    """Load transformed data to target"""
    click.echo(f"📦 Loading data to {target}")
    df = pd.read_csv('transformed_sql.csv')
    df.to_csv(target, index=False)
    click.echo("✅ Load complete")

# ------------------ Report ------------------
@cli.command()
@click.option('--input', default='transformed_sql.csv', help='Input file for report')
def report(input):
    """Generate summary report"""
    click.echo(f"📊 Generating report for {input}")
    df = pd.read_csv(input)
    click.echo(df.describe(include='all'))

# ------------------ Validate ------------------
@cli.command()
@click.option('--input', default='transformed_sql.csv', help='File to validate')
def validate(input):
    """Validate data quality"""
    click.echo(f"🔍 Validating {input}")
    df = pd.read_csv(input)

    issues = []
    if df.isnull().values.any():
        issues.append("❗ Null values found")
    if df.duplicated().any():
        issues.append("❗ Duplicate rows found")

    if issues:
        for issue in issues:
            click.echo(issue)
    else:
        click.echo("✅ Data passed validation")

if __name__ == '__main__':
    cli()