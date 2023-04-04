"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    df = pd.read_csv('clusters_report.txt', sep='\t', header=[0,1])
    cols = df.columns
    df.drop([0], axis=0, inplace=True)
    df.columns = ["0"]
       
    
    for let in df:
        df[let] = df[let].str.replace('  ', ' ')
                     
    df2 = df["0"].str.split(' ', n=14, expand=True)
   
    df2.drop([0,1,3,6,7,8,9,10], axis=1, inplace=True)
    
    df2["seg"]=df2[4] + df2[5]

    
    df2["ter"]=df2[11] + df2[12] + df2[13]
    df2.drop([4,5,11,12,13], axis=1, inplace=True)
    df2=df2[[2,'seg','ter',14]]   

    df2["ter"] = df2["ter"].str.replace("%", "")
    df2["ter"] = df2["ter"].str.replace(",", ".")

    count = 1
    for num in df2[2]:
        try:
            df2[2][count] = int(num)
        except:
            df2[2][count] = 0       
        count += 1
    
    count = 1
    for num in df2[2]:
        if num != 0:
            a=num
        else: 
            df2[2][count] = a
        count=count+1 
    
    df2=df2.groupby(2).sum()
    
    df2.reset_index(drop=False, inplace=True)
    
    df2[14] = df2[14].str.replace(",", ", ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace("  ", " ")
    df2[14] = df2[14].str.replace(".", "",regex=False)
    df2[14] = df2[14].str.strip()

    df2.rename(columns={
    2: 'cluster',
    'seg': 'cantidad_de_palabras_clave',
    'ter': 'porcentaje_de_palabras_clave',
    14: 'principales_palabras_clave'},
    inplace=True
    )

    df2 = df2.astype({
        'cluster': 'int64',
        'cantidad_de_palabras_clave': 'int64', 
        'porcentaje_de_palabras_clave': 'float64',
        'principales_palabras_clave': 'object'
    })
    
    
        
    return df2

#     print(df2.head(14))
#     print(df2.columns)


# if __name__ == '__main__':
#     ingest_data()
