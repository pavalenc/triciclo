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
        
def get_rdd_distict_edges(sc, filename):
    return sc.textFile(filename).map(get_edges).filter(lambda x: x is not None).distinct() 

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

def triciclos_apartado1(sc,filename):
    edges = get_rdd_distict_edges(sc,filename)
    asociados = edges.groupByKey().mapValues(list).flatMap(funcion_auxiliar)
    triciclos = asociados.groupByKey().mapValues(list).filter(filtracion).flatMap(coloca_ternas)
    print(triciclos.collect())
    return triciclos.collect()
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        triciclos_apartado1(sc,sys.argv[1])