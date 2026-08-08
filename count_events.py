"""
count how often each event/activity appears

"""
import argparse
import pm4py

ap = argparse.ArgumentParser()
ap.add_argument("--xes", required=True, help="event log .xes")
args = ap.parse_args()

log = pm4py.read_xes(args.xes)
df = pm4py.convert_to_dataframe(log)
counts = (df.groupby(["case:concept:name", "concept:name"]).size().unstack(fill_value=0))
print(counts.describe(percentiles=[.5, .9, .95, .99]).T)
