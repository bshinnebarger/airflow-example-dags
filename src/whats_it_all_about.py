import pandas as pd

def meaning_of_life():
    df_hmm = pd.DataFrame(data=[(2, 7), (6, 13), (11, 4), (2, 16)])
    print(f'The answer is: {df_hmm.product(axis=1).mean()}')