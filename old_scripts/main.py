import inspect, data_creator


def main():
	print 'Starbird Mongo Datasets: V 1.0'
	for (i,x) in enumerate(inspect.getmembers(data_creator, inspect.isfunction)):
		print '[%s] %s' % (i,x[0])
	option = int(raw_input('>> '))
	inspect.getmembers(data_creator, inspect.isfunction)[option][1]()

if __name__ == "__main__":
    main()