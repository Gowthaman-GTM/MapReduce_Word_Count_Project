"""
Part C - MapReduce-style Word Count with Graphical User Interface (GUI)
Cloud Programming Model Implementation

This program simulates distributed MapReduce workers locally using multiprocessing
and provides a simple Tkinter UI to select any text file from storage.
"""

from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import sys
import re
import os

# --- CORE MAPREDUCE FUNCTIONS ---

def map_worker(text_chunk):
    """Map phase: convert words into (word, 1) key-value pairs."""
    words = re.findall(r"[A-Za-z0-9']+", text_chunk.lower())
    return [(word, 1) for word in words]


def reduce_worker(items):
    """Reduce phase: sum the values for one group of words."""
    word, values = items
    return word, sum(values)


def split_text(text, num_parts):
    """Split input text into approximately equal line-based chunks."""
    lines = text.splitlines()
    if not lines:
        return []

    num_parts = max(1, min(num_parts, len(lines)))
    chunk_size = (len(lines) + num_parts - 1) // num_parts
    return [
        "\n".join(lines[i:i + chunk_size])
        for i in range(0, len(lines), chunk_size)
    ]


def run_map_stage(chunks, workers):
    with ProcessPoolExecutor(max_workers=workers) as executor:
        mapped = list(executor.map(map_worker, chunks))
    return [pair for worker_result in mapped for pair in worker_result]


def shuffle_stage(mapped_pairs):
    """Shuffle/group phase: group all values belonging to the same word."""
    grouped = defaultdict(list)
    for word, value in mapped_pairs:
        grouped[word].append(value)
    return sorted(grouped.items())


def run_reduce_stage(grouped_items, workers):
    with ProcessPoolExecutor(max_workers=workers) as executor:
        reduced = list(executor.map(reduce_worker, grouped_items))
    return sorted(reduced, key=lambda x: (-x[1], x[0]))


# --- CLI MODE ---

def run_cli(input_path="input.txt", output_path="output.txt", workers=None):
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        print(f"Error: {input_file} was not found.")
        sys.exit(1)

    text = input_file.read_text(encoding="utf-8", errors="ignore")

    cpu_count = os.cpu_count() or 2
    if workers is None:
        workers = min(4, max(2, cpu_count))

    chunks = split_text(text, workers)

    print("=" * 60)
    print("MAPREDUCE-STYLE WORD COUNT")
    print("=" * 60)
    print(f"Input file       : {input_file}")
    print(f"Worker processes : {workers}")
    print(f"Input partitions : {len(chunks)}")

    mapped_pairs = run_map_stage(chunks, workers)
    print(f"Map pairs created: {len(mapped_pairs)}")

    grouped_items = shuffle_stage(mapped_pairs)
    print(f"Unique words     : {len(grouped_items)}")

    results = run_reduce_stage(grouped_items, workers)

    with output_file.open("w", encoding="utf-8") as f:
        for word, count in results:
            f.write(f"{word}\t{count}\n")

    print("\nFinal word counts:")
    for word, count in results:
        print(f"{word:15} {count}")

    print(f"\nOutput saved to : {output_file}")
    print("=" * 60)


# --- TKINTER GUI APP ---

class MapReduceApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MapReduce Word Count Processor")
        self.root.geometry("820x680")
        self.root.minsize(700, 550)

        # Style configuration
        self.style = ttk.Style()
        if "clam" in self.style.theme_names():
            self.style.theme_use("clam")

        # Custom colors & styles
        self.root.configure(bg="#f4f6f9")

        default_input = str(Path("input.txt").resolve()) if Path("input.txt").exists() else ""
        self.input_file_path = tk.StringVar(value=default_input)
        self.workers_var = tk.IntVar(value=min(4, max(2, os.cpu_count() or 2)))
        self.status_var = tk.StringVar(value="Ready. Please select a text file to process.")

        self.results_data = []

        self._build_ui()

    def _build_ui(self):
        # Header Banner
        header_frame = tk.Frame(self.root, bg="#1e293b", padx=15, pady=12)
        header_frame.pack(fill="x")
        
        title_label = tk.Label(
            header_frame, 
            text="⚡ MapReduce Word Count Processor", 
            font=("Segoe UI", 16, "bold"), 
            fg="#ffffff", 
            bg="#1e293b"
        )
        title_label.pack(anchor="w")
        
        subtitle_label = tk.Label(
            header_frame, 
            text="Simulate parallel Map, Shuffle, and Reduce stages on local text files", 
            font=("Segoe UI", 10), 
            fg="#94a3b8", 
            bg="#1e293b"
        )
        subtitle_label.pack(anchor="w", pady=(2, 0))

        # Main Container
        main_frame = ttk.Frame(self.root, padding=15)
        main_frame.pack(fill="both", expand=True)

        # 1. File Selection Section
        file_frame = ttk.LabelFrame(main_frame, text=" 📂 Input File Selection ", padding=12)
        file_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(file_frame, text="Text File Path:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w", padx=(0, 8))
        
        file_entry = ttk.Entry(file_frame, textvariable=self.input_file_path, font=("Segoe UI", 10))
        file_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        
        browse_btn = ttk.Button(file_frame, text="Browse Storage...", command=self.browse_file)
        browse_btn.grid(row=0, column=2, sticky="e")

        file_frame.columnconfigure(1, weight=1)

        # 2. Execution Settings Section
        settings_frame = ttk.LabelFrame(main_frame, text=" ⚙️ Processing Configuration ", padding=12)
        settings_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(settings_frame, text="Worker Processes (CPU Cores):", font=("Segoe UI", 10)).grid(row=0, column=0, sticky="w", padx=(0, 8))
        
        max_cpus = os.cpu_count() or 8
        workers_spin = ttk.Spinbox(settings_frame, from_=1, to=max_cpus, textvariable=self.workers_var, width=6, font=("Segoe UI", 10))
        workers_spin.grid(row=0, column=1, sticky="w", padx=(0, 20))

        self.run_btn = ttk.Button(settings_frame, text="▶ Run MapReduce Word Count", command=self.start_processing)
        self.run_btn.grid(row=0, column=2, sticky="e")

        settings_frame.columnconfigure(2, weight=1)

        # 3. Metrics Summary Frame
        metrics_frame = ttk.Frame(main_frame)
        metrics_frame.pack(fill="x", pady=(0, 10))

        self.lbl_partitions = self._create_metric_card(metrics_frame, "Partitions", "0", 0)
        self.lbl_total_words = self._create_metric_card(metrics_frame, "Total Words", "0", 1)
        self.lbl_unique_words = self._create_metric_card(metrics_frame, "Unique Words", "0", 2)
        
        metrics_frame.columnconfigure(0, weight=1)
        metrics_frame.columnconfigure(1, weight=1)
        metrics_frame.columnconfigure(2, weight=1)

        # 4. Results Notebook (Tabs for Word Count Table & Log Output)
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill="both", expand=True, pady=(0, 10))

        # Tab 1: Table View
        table_tab = ttk.Frame(notebook, padding=8)
        notebook.add(table_tab, text=" 📊 Word Count Results ")

        # Search Bar
        search_frame = ttk.Frame(table_tab)
        search_frame.pack(fill="x", pady=(0, 6))
        ttk.Label(search_frame, text="Search Word:", font=("Segoe UI", 9)).pack(side="left", padx=(0, 6))
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self.filter_table)
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, font=("Segoe UI", 9))
        search_entry.pack(side="left", fill="x", expand=True)

        # Treeview Table
        columns = ("rank", "word", "count")
        self.tree = ttk.Treeview(table_tab, columns=columns, show="headings", selectmode="browse")
        self.tree.heading("rank", text="#", anchor="center")
        self.tree.heading("word", text="Word", anchor="w")
        self.tree.heading("count", text="Frequency Count", anchor="center")

        self.tree.column("rank", width=60, minwidth=50, stretch=False, anchor="center")
        self.tree.column("word", width=350, minwidth=150, anchor="w")
        self.tree.column("count", width=150, minwidth=100, anchor="center")

        tree_scroll = ttk.Scrollbar(table_tab, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)

        self.tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        # Tab 2: Execution Log
        log_tab = ttk.Frame(notebook, padding=8)
        notebook.add(log_tab, text=" 📝 Execution Log ")

        self.log_text = scrolledtext.ScrolledText(log_tab, font=("Consolas", 10), state="disabled", bg="#0f172a", fg="#e2e8f0")
        self.log_text.pack(fill="both", expand=True)

        # 5. Bottom Bar (Status & Export)
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(fill="x")

        self.progress_bar = ttk.Progressbar(bottom_frame, mode="indeterminate")
        self.progress_bar.pack(fill="x", pady=(0, 6))

        status_lbl = ttk.Label(bottom_frame, textvariable=self.status_var, font=("Segoe UI", 9, "italic"))
        status_lbl.pack(side="left")

        save_btn = ttk.Button(bottom_frame, text="💾 Export Results...", command=self.save_output_file)
        save_btn.pack(side="right")

    def _create_metric_card(self, parent, title, initial_value, col):
        card = tk.Frame(parent, bg="#ffffff", highlightbackground="#cbd5e1", highlightthickness=1, padx=12, pady=8)
        card.grid(row=0, column=col, sticky="ew", padx=4)
        
        lbl_title = tk.Label(card, text=title.upper(), font=("Segoe UI", 8, "bold"), fg="#64748b", bg="#ffffff")
        lbl_title.pack(anchor="w")
        
        lbl_val = tk.Label(card, text=initial_value, font=("Segoe UI", 16, "bold"), fg="#0f172a", bg="#ffffff")
        lbl_val.pack(anchor="w")
        return lbl_val

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Text File for Word Count",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if file_path:
            self.input_file_path.set(file_path)
            self.status_var.set(f"Selected file: {Path(file_path).name}")

    def log(self, message):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def start_processing(self):
        input_path_str = self.input_file_path.get().strip()
        if not input_path_str:
            messagebox.showwarning("No File Selected", "Please select a text file from storage first.")
            return

        input_file = Path(input_path_str)
        if not input_file.exists():
            messagebox.showerror("File Not Found", f"The file '{input_path_str}' does not exist.")
            return

        workers = self.workers_var.get()

        # Disable button & start progress bar
        self.run_btn.configure(state="disabled")
        self.progress_bar.start(10)
        self.status_var.set("Processing MapReduce job across workers...")

        self.clear_log()
        self.log("=" * 55)
        self.log("STARTING MAPREDUCE WORD COUNT PROCESS")
        self.log("=" * 55)
        self.log(f"Input File       : {input_file}")
        self.log(f"Worker Processes : {workers}")

        # Run background thread so UI stays responsive
        thread = threading.Thread(target=self._run_mapreduce_thread, args=(input_file, workers), daemon=True)
        thread.start()

    def _run_mapreduce_thread(self, input_file, workers):
        try:
            text = input_file.read_text(encoding="utf-8", errors="ignore")
            chunks = split_text(text, workers)

            self.root.after(0, self.log, f"Input Partitions : {len(chunks)}")
            self.root.after(0, self.log, "\n[STAGE 1] Running Map Workers...")
            mapped_pairs = run_map_stage(chunks, workers)
            self.root.after(0, self.log, f" -> Map Stage complete. Pairs created: {len(mapped_pairs)}")

            self.root.after(0, self.log, "\n[STAGE 2] Running Shuffle & Grouping...")
            grouped_items = shuffle_stage(mapped_pairs)
            self.root.after(0, self.log, f" -> Shuffle Stage complete. Unique keys: {len(grouped_items)}")

            self.root.after(0, self.log, "\n[STAGE 3] Running Reduce Workers...")
            results = run_reduce_stage(grouped_items, workers)
            self.root.after(0, self.log, f" -> Reduce Stage complete. Total result items: {len(results)}")

            # Auto-save to output.txt
            out_file = Path("output.txt")
            with out_file.open("w", encoding="utf-8") as f:
                for word, count in results:
                    f.write(f"{word}\t{count}\n")
            self.root.after(0, self.log, f"\nSaved results to: {out_file.resolve()}")

            # Schedule UI update
            self.root.after(0, self._on_processing_complete, len(chunks), len(mapped_pairs), results)

        except Exception as e:
            self.root.after(0, self._on_processing_error, str(e))

    def _on_processing_complete(self, num_chunks, total_words, results):
        self.progress_bar.stop()
        self.run_btn.configure(state="normal")
        self.status_var.set("✅ Processing completed successfully!")

        self.results_data = results

        # Update Metric Cards
        self.lbl_partitions.configure(text=str(num_chunks))
        self.lbl_total_words.configure(text=str(total_words))
        self.lbl_unique_words.configure(text=str(len(results)))

        # Update Treeview Table
        self.populate_table(results)

        messagebox.showinfo("Success", f"MapReduce completed!\n\nProcessed {total_words} words into {len(results)} unique terms.\nResults saved to output.txt")

    def _on_processing_error(self, error_msg):
        self.progress_bar.stop()
        self.run_btn.configure(state="normal")
        self.status_var.set("❌ Processing failed.")
        self.log(f"\nERROR: {error_msg}")
        messagebox.showerror("Execution Error", f"An error occurred during execution:\n{error_msg}")

    def populate_table(self, data):
        # Clear existing rows
        for item in self.tree.get_children():
            self.tree.delete(item)

        for rank, (word, count) in enumerate(data, start=1):
            self.tree.insert("", "end", values=(rank, word, count))

    def filter_table(self, *args):
        query = self.search_var.get().strip().lower()
        if not query:
            self.populate_table(self.results_data)
        else:
            filtered = [(w, c) for w, c in self.results_data if query in w.lower()]
            self.populate_table(filtered)

    def save_output_file(self):
        if not self.results_data:
            messagebox.showwarning("No Results", "No results available to export. Run the process first.")
            return

        file_path = filedialog.asksaveasfilename(
            title="Export Word Count Results",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                if file_path.endswith(".csv"):
                    f.write("Word,Count\n")
                    for word, count in self.results_data:
                        f.write(f'"{word}",{count}\n')
                else:
                    for word, count in self.results_data:
                        f.write(f"{word}\t{count}\n")
            messagebox.showinfo("Export Saved", f"Results successfully exported to:\n{file_path}")


def main():
    if "--cli" in sys.argv:
        run_cli()
    else:
        root = tk.Tk()
        app = MapReduceApp(root)
        root.mainloop()


if __name__ == "__main__":
    main()

