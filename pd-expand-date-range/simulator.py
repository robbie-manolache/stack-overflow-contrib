
from datetime import datetime
import numpy as np
import pandas as pd
from date_expander import DateExpander


class Simulator(DateExpander):
    
    def __init__(
        self,
        n_rows: int,
        n_periods: int,
        n_sims: int
    ) -> None:
        
        df = pd.DataFrame({
            "id": "ID"+pd.Series(range(n_rows)).astype(str)\
                .str.zfill(len(str(n_rows))+1),
            "start": pd.to_datetime(1.6e9+np.random.randint(
                1, 5.2e7, n_rows), unit="s").round("H")
        })
        
        df["end"] = df["start"] + pd.Timedelta(days=n_periods)
        
        DateExpander.__init__(
            self,
            dataframe=df,
            start_dt_colname="start",
            end_dt_colname="end",
            time_unit="D",
            new_colname="date",
            id_colname="id",
            end_inclusive=True
        )
        
        self.n_sims = n_sims
        self.results = None
        
    def run(self):
        
        results = []
        methods = ["jwdink", "TedPetrou", "Gen", "Gen2", "robbie"]
                
        for n in range(self.n_sims):
            
            sim_record = {}
            
            for m in methods:
                
                t0 = datetime.now() 
                _ = getattr(self, m)()
                sim_record[m] = datetime.now() - t0
                
            results.append(sim_record)
        
        self.results = results
                          
