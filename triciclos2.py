import sys
from pyspark import SparkContext
sc = SparkContext()

def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass 
        

def funcion_auxiliar(tupla):
    result = []
    for i in range(len(tupla[1])):
        result.append(((tupla[0],tupla[1][i]),'exists'))
        for j in range(i+1,len(tupla[1])):
            if tupla[1][i] < tupla[1][j]:
                result.append(((tupla[1][i],tupla[1][j]),('pending',tupla[0])))
            else:
                result.append(((tupla[1][j],tupla[1][i]),('pending',tupla[0])))
    return result

def filtracion(tupla):
    return (len(tupla[1])>= 2 and 'exists' in tupla[1])

    
def coloca_ternas(tupla):
    result = []
    for elem in tupla[1]:
        if elem != 'exists':
            result.append((elem[1],tupla[0][0], tupla[0][1]))
    return result

    
def triciclos_apartado2(sc, files):
    rdd = sc.parallelize([])
    for file in files:
        file_rdd = sc.textFile(file)
        rdd = rdd.union(file_rdd)
    adj = rdd.map(get_edges).filter(lambda x: x is not None).distinct()
    asociados = adj.groupByKey().mapValues(list).flatMap(funcion_auxiliar)
    triciclos = asociados.groupByKey().mapValues(list).filter(filtracion).flatMap(coloca_ternas)
    print(triciclos.collect())
    return triciclos.collect()

if __name__ == "__main__":
    lista = []
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                lista.append(sys.argv[i])
        triciclos_apartado2(sc,lista)