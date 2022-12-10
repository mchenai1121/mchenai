import pandas as pd
import tabula

frames = tabula.read_pdf("https://github.com/mchenai1121/mchenai/raw/main/con_9Dec22.pdf", lattice = True, pages = "all")

a = frames[0]
b = a.iloc[0]
print(b)

for i in frames:
    i.columns = range(i.shape[1])
    i.drop(0, axis=0, inplace = True)

df = pd.concat(frames, ignore_index = True)


