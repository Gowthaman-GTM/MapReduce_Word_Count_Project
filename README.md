# Part C - MapReduce Word Count

## Files
- `mapreduce_wordcount.py` - working Python implementation
- `input.txt` - sample input dataset
- `output.txt` - generated word-count results
- `run_output.txt` - sample console execution
- `Part_C_Report_500_800_Words.docx` - report draft

## How to run

1. Install Python 3.10+.
2. Open a terminal in this folder.
3. Launch the graphical user interface (GUI):

```bash
python mapreduce_wordcount.py
```

This opens a Simple UI window where you can:
- Click **"Browse Storage..."** to choose any `.txt` file from your computer.
- Adjust the number of worker CPU cores.
- Click **"▶ Run MapReduce Word Count"**.
- View word count results in an interactive table, search words, view logs, and export results.

Alternatively, to run directly in CLI terminal mode:
```bash
python mapreduce_wordcount.py --cli
```

The program automatically updates `output.txt`.

## What to screenshot
1. The Python source code showing Map, Shuffle, and Reduce functions.
2. The terminal showing `MAPREDUCE-STYLE WORD COUNT`, worker processes, and final results.
3. The `output.txt` file showing the final word counts.
4. If possible, show the project folder containing the input, code, output, and report.

## Important
This is a local simulation of distributed workers using Python processes. It is not a real cloud cluster. The report explains how the same MapReduce model could scale to a cloud deployment and the limitations of the local simulation.
