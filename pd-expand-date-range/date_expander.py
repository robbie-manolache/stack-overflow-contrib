
import numpy as np
import pandas as pd


class DateExpander:

    def __init__(
        self,
        dataframe: pd.DataFrame,
        start_dt_colname: str,
        end_dt_colname: str,
        time_unit: str,
        new_colname: str,
        id_colname: str,
        end_inclusive: bool
    ) -> None:

        self.df = dataframe
        self.start = start_dt_colname
        self.end = end_dt_colname
        self.freq = time_unit
        self.new_name = new_colname
        self.id_col = id_colname
        self.include_end = end_inclusive

        if end_inclusive:
            self.closed = None
        else:
            self.closed = "left"

    def jwdink(self) -> pd.DataFrame:

        td = pd.Timedelta(1, self.freq)

        # add a timediff column:
        self.df['_dt_diff'] = self.df[self.end] - self.df[self.start]

        # get the maximum timediff:
        max_diff = int((self.df['_dt_diff'] / td).max())

        # for each possible timediff, get the intermediate time-differences:
        df_diffs = pd.concat([
            pd.DataFrame({
                '_to_add': np.arange(0, dt_diff + self.include_end)*td
            }).assign(_dt_diff=dt_diff * td)
            for dt_diff in range(max_diff + 1)
        ])

        # join to the original dataframe
        new_df = self.df.merge(df_diffs, on='_dt_diff')

        # the new dt column is just start plus the intermediate diffs:
        new_df[self.new_name] = new_df[self.start] + new_df['_to_add']

        # remove start-end cols, as well as temp cols used for calculations:
        to_drop = [self.start, self.end, '_to_add', '_dt_diff']
        if self.new_name in to_drop:
            to_drop.remove(self.new_name)
        new_df = new_df.drop(columns=to_drop)

        # don't modify dataframe in place:
        del self.df['_dt_diff']

        return new_df

    def TedPetrou(self) -> pd.DataFrame:

        return pd.concat([pd.DataFrame({
            self.new_name: pd.date_range(row[self.start], row[self.end],
                                         freq=self.freq, closed=self.closed),
            self.id_col: row[self.id_col]
        }) for i, row in self.df.iterrows()], ignore_index=True)

    def Gen(self) -> pd.DataFrame:

        new_df = self.df.melt(
            id_vars=self.id_col,
            value_name=self.new_name
        ).drop("variable", axis=1)

        return new_df.groupby(self.id_col).apply(
            lambda grp: grp.set_index(self.new_name)
            .resample(self.freq, closed=self.closed).pad()
        ).reset_index(level=1).reset_index(drop=True)

    def Gen2(self) -> pd.DataFrame:

        new_df = self.df.melt(
            id_vars=self.id_col,
            value_name=self.new_name
        ).drop("variable", axis=1)

        return new_df.set_index(self.new_name).groupby(self.id_col)\
            .resample(self.freq, closed=self.closed).ffill()\
            .reset_index(level=1).reset_index(drop=True)
            
    def robbie(self) -> pd.DataFrame:
        
        return self.df.set_index(self.id_col).apply(
            lambda r: pd.date_range(
                r[self.start], r[self.end], 
                freq=self.freq, closed=self.closed
            ).values, axis=1
        ).explode().rename(self.new_name).reset_index()
