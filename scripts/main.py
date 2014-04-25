import inspect, basic_graphs
correction = 5

def main():
	print 'Starbird Mongo Datasets: V 1.0'
	print 'please select an option (0 to exit):'
	for (i,x) in enumerate(inspect.getmembers(basic_graphs, inspect.isfunction)):
		if x[0][0] != '_':
			print '[%s] %s' % (i - correction,x[0])
	option = int(raw_input('>> ')) + correction
	if option is not correction:
		inspect.getmembers(basic_graphs, inspect.isfunction)[option][1]()

if __name__ == "__main__":
        main()
