# main.py
import pandas as pd
import plotly.express as px
from utils import multiply, create_sequence

# Generate data
x_values = create_sequence(1, 5)
y_values = [multiply(n, 10) for n in x_values]

# Create DataFrame
df = pd.DataFrame({"x": x_values, "y": y_values})

# Plot chart
fig = px.line(df, x="x", y="y", title="Pyodide Multi-file Chart Example")

# Save chart HTML
fig.write_html("/plot.html")
