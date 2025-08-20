import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

data = [
    {"Threads": 4, "Connections": 100, "Implementation": "Tokio", "Requests/sec": 3271, "Avg Latency": 0.0306, "P99 Latency": 0.0374},
    {"Threads": 4, "Connections": 1000, "Implementation": "Tokio", "Requests/sec": 3248, "Avg Latency": 0.3077, "P99 Latency": 0.3219},
    {"Threads": 4, "Connections": 100, "Implementation": "uring", "Requests/sec": 3267, "Avg Latency": 0.0306, "P99 Latency": 0.0375},
    {"Threads": 4, "Connections": 1000, "Implementation": "uring", "Requests/sec": 3237, "Avg Latency": 0.3088, "P99 Latency": 0.3545},
    {"Threads": 4, "Connections": 100, "Implementation": "uring+sqpoll", "Requests/sec": 3271, "Avg Latency": 0.0306, "P99 Latency": 0.0380},
    {"Threads": 4, "Connections": 1000, "Implementation": "uring+sqpoll", "Requests/sec": 3242, "Avg Latency": 0.3083, "P99 Latency": 0.3220},

    {"Threads": 6, "Connections": 100, "Implementation": "Tokio", "Requests/sec": 3268, "Avg Latency": 0.0306, "P99 Latency": 0.0377},
    {"Threads": 6, "Connections": 1000, "Implementation": "Tokio", "Requests/sec": 3242, "Avg Latency": 0.3082, "P99 Latency": 0.3226},
    {"Threads": 6, "Connections": 100, "Implementation": "uring", "Requests/sec": 3264, "Avg Latency": 0.0306, "P99 Latency": 0.0373},
    {"Threads": 6, "Connections": 1000, "Implementation": "uring", "Requests/sec": 3243, "Avg Latency": 0.3082, "P99 Latency": 0.3211},
    {"Threads": 6, "Connections": 100, "Implementation": "uring+sqpoll", "Requests/sec": 3273, "Avg Latency": 0.0305, "P99 Latency": 0.0371},
    {"Threads": 6, "Connections": 1000, "Implementation": "uring+sqpoll", "Requests/sec": 3239, "Avg Latency": 0.3085, "P99 Latency": 0.3203},
]

df = pd.DataFrame(data)

sns.set(style="whitegrid")

def plot_metric(metric_name):
    g = sns.catplot(
        data=df,
        x="Connections",
        y=metric_name,
        hue="Implementation",
        col="Threads",
        kind="bar",
        errorbar=None,
        height=5,
        aspect=1
    )
    g.set_axis_labels("Connections", metric_name)
    g.set_titles("Threads = {col_name}")
    plt.subplots_adjust(top=0.85)
    g.fig.suptitle(f"{metric_name}")
    
    filename = metric_name.replace(" ", "_").replace("/", "-") + ".png"
    plt.savefig(filename, bbox_inches="tight")
    print(f"Saved: {filename}")
    plt.close()

plot_metric("Requests/sec")
plot_metric("Avg Latency")
plot_metric("P99 Latency")
