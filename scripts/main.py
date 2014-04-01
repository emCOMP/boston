import inspect, basic_graphs

def main():
        print 'Starbird Mongo Datasets: V 1.0'
        count = 0
        for x in inspect.getmembers(basic_graphs, inspect.isfunction):
                if x[0][0] != '_' and x[0] != 'main':
                        print '[%s] %s' % (count,x[0])
                        count += 1
        option = int(raw_input('>> '))
        inspect.getmembers(basic_graphs, inspect.isfunction)[option][1]()

if __name__ == "__main__":
        main()
