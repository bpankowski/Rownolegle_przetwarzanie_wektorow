#include <mpi.h>
#include <stdio.h>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <math.h>
#include <sstream>

using namespace std;

typedef struct vector_h
{
	double x;
	double y;
	double z;

	vector_h(double _x, double _y, double _z)
	{
		x = _x;
		y = _y;
		z = _z;
	}
} vector_s;

void metoda1(int rank, int size, double& timeData1, int& dataSize, double sendBuf[4], double& timeProcess1)
{
	vector<vector_s> data;
	double startData = 0, endData = 0;

	startData = MPI_Wtime();
	fstream input("v04.dat", std::fstream::in | std::fstream::binary);

	if (!input.is_open()) //wczytanie danych z pliku do wektora
	{
		cout << "error loading file" << std::endl;

		return;
	}

	double xD, yD, zD;
	while (input >> xD >> yD >> zD)
	{
		data.push_back(vector_s(xD, yD, zD));
	}
	endData = MPI_Wtime();	
	input.close();

	timeData1 = endData - startData;

	//end of data input
	dataSize = data.size();
	int n = data.size() / size, mod = data.size() % size, number = 0;
	double _v = 0, l = 0, _l = 0, x = 0, _x = 0, y = 0, _y = 0, z = 0, _z = 0;

	double startProcess=0, endProcess=0;
	startProcess = MPI_Wtime();

	for (int i = rank*n; number < n; i++, number++)
	{
		l += sqrt(data.at(i).x*data.at(i).x + data.at(i).y*data.at(i).y + data.at(i).z*data.at(i).z);
		x += data.at(i).x;
		y += data.at(i).y;
		z += data.at(i).z;
	}

	if ((mod > 0) && (rank < mod))
	{
		l += sqrt(data.at(n*rank + rank).x*data.at(n*rank + rank).x +
			data.at(n*rank + rank).y*data.at(n*rank + rank).y +
			data.at(n*rank + rank).z*data.at(n*rank + rank).z);
		x += data.at(n*rank + rank).x;
		y += data.at(n*rank + rank).y;
		z += data.at(n*rank + rank).z;
		number++;
	}

	//cout << "number " << number << " l " << l << " mod " << mod << endl;

	if (number > 0)
	{
		_l = l / number;
		_x = x / number;
		_y = y / number;
		_z = z / number;
	}

	endProcess = MPI_Wtime();
	timeProcess1 = endProcess - startProcess;

	//cout << "_l " << _l << " _x " << _x << " _y " << _y << " _z " << _z << endl;

	sendBuf[0] = _l;
	sendBuf[1] = _x;
	sendBuf[2] = _y;
	sendBuf[3] = _z;
}

void metoda2(int rank, int size, double& timeData2, int& dataSize, double sendBuf[4], double& timeProcess2)
{
	std::vector<vector_s> data;
	MPI_File file;

	double startProcess = 0, endProcess = 0, startData = 0, endData = 0;
	startData = MPI_Wtime();
	//otworz plik
	MPI_File_open(MPI_COMM_WORLD, "v04.dat", MPI_MODE_RDONLY, MPI_INFO_NULL, &file);

	//skumaj jaki offset zrobic
	MPI_Offset fileSize;
	MPI_File_get_size(file, &fileSize);
	//wielkosc

	int lineLength = 40;
	int lineNumber = (int)fileSize / lineLength;
	//cout << lineNumber << "  line" << endl;
	dataSize = lineNumber;

	int sizePerRank = lineNumber / size;
	int mod = lineNumber % size;
	int start = rank * sizePerRank, end;

	if (mod > 0)
	{
		if (rank < mod)
		{
			start += rank;
			end = start + sizePerRank + 1;
			//plus element z modulo
		}
		else
		{
			start += mod;
			end = start + sizePerRank;
		}
	}
	else
	{
		end = start + sizePerRank;
	}

//	cout << "lines per rank " << sizePerRank << endl;
	//cout << "mod " << mod << endl;
	//cout << "start " << start * lineLength << " end " << end * lineLength << endl;

	//poczytaj z pliku
	char * buffer = new char[(end - start) * lineLength + 1];
	MPI_File_read_at_all(file, start * lineLength, buffer, end * lineLength - start * lineLength, MPI_CHAR, MPI_STATUS_IGNORE);

	stringstream ss;
	ss << buffer;

	double x, y, z;
	//cout << "tu2 " << endl;
	while (ss >> x >> y >> z)
	{
		data.push_back(vector_s(x, y, z));
	}
	endData = MPI_Wtime();

	timeData2 = endData - startData;
	//cout << "tu1 " << endl;
	//policz
	double l = 0;
	x = 0;
	y = 0;
	z = 0;
	startProcess = MPI_Wtime();
	for (int i = 0; i < data.size(); i++)
	{
		l += sqrt(data.at(i).x*data.at(i).x + data.at(i).y*data.at(i).y + data.at(i).z*data.at(i).z);
		x += data.at(i).x;
		y += data.at(i).y;
		z += data.at(i).z;
	}
	endProcess = MPI_Wtime();
	timeProcess2 = endProcess - startProcess;

	//cout << "rank " << rank << " l " << l / (end - start) << " x " << x / (end - start) << " y " << y / (end - start) << " z " << z / (end - start) << endl;

	sendBuf[0] = l / (end - start);
	sendBuf[1] = x / (end - start);
	sendBuf[2] = y / (end - start);
	sendBuf[3] = z / (end - start);

	MPI_File_close(&file);
}

int main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);
	int size, rank;
	//MPI_Comm_size(MPI_COMM_WORLD, &size);
	//MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	int data;
	rank = 0;
	size = 3;
	double sendBuf[4];

	double timeProcess = 0, timeData = 0;
	double startReduce = 0, endReduce = 0;
	
	char wybor;
		
	if (argc < 2)
	{
		cout << "niepoprawna ilosc argumentow" << endl;
		return -1;
	}

	wybor = argv[1][0];
		
		switch (wybor)
		{
		
		case '0':

			metoda1(rank, size, timeData, data, sendBuf, timeProcess);
			break;

		case '1':

			metoda2(rank, size, timeData, data, sendBuf, timeProcess);
			break;

		}
	
	double lSum = 0, xSum = 0, ySum = 0, zSum = 0;
	double *recvBufSum;
	recvBufSum = (double*)malloc(4 * size * sizeof(double));

	startReduce = MPI_Wtime();
	MPI_Gather(sendBuf, 4, MPI_DOUBLE, recvBufSum, 4, MPI_DOUBLE, 0, MPI_COMM_WORLD);
	endReduce = MPI_Wtime();
	//cout << "rank" <<" " << rank<< " " << "reduce" << " " << endReduce - startReduce << " " << "time process" << " " << timeProcess << endl;
	double *recvBufTime, sendBuff[3];
	sendBuff[0] = timeData;
	sendBuff[1] = timeProcess;
	sendBuff[2] = endReduce - startReduce;
	recvBufTime = (double*)malloc(3 * size * sizeof(double));

	MPI_Gather(sendBuff, 3, MPI_DOUBLE, recvBufTime, 3, MPI_DOUBLE, 0, MPI_COMM_WORLD);

	if (rank == 0)
	{
		for (int i = 0; i < 4 * size; i += 4)
		{
			lSum += recvBufSum[i];
			xSum += recvBufSum[i + 1];
			ySum += recvBufSum[i + 2];
			zSum += recvBufSum[i + 3];

		//	cout << "recvBufSuml " << recvBufSum[i] << " recvBufSumx " << recvBufSum[i + 1] << " recvBufSumy " << recvBufSum[i + 2] << " recvBufSumz " << recvBufSum[i + 3] << endl;
		}

		int mod = data % size;

		if (data > size)
			cout << "srednie l = " << lSum / size << " , srednie r = [" << xSum / size << ", " << ySum / size << ", " << zSum / size << "]" << endl;
		else
			cout << "srednie l = " << lSum / mod << " , srednie r = [" << xSum / mod << ", " << ySum / mod << ", " << zSum / mod << "]" << endl;
			fstream timings("timings.txt", fstream::out);

		if (!timings.is_open())
		{
			cout << "error loading file" << endl;
			MPI_Finalize();
			return -1;
		}

		double totalData = 0, totalProcess = 0, totalReduce = 0, total = 0;
		for (int i = 0, j = 0; j < size; i += 3, j++)
		{
			timings << "timings (proc " << j << "):" << endl;
			timings << "\treadData: " << recvBufTime[i] << endl;
			timings << "\tprocessData: " << recvBufTime[i + 1] << endl;
			timings << "\treduceResults: " << recvBufTime[i + 2] << endl;
			timings << "\ttotal: " << recvBufTime[i] + recvBufTime[i + 1] + recvBufTime[i + 2] << endl;
			timings << endl;

			totalData += recvBufTime[i];
			totalProcess += recvBufTime[i + 1];
			totalReduce += recvBufTime[i + 2];
			total += recvBufTime[i] + recvBufTime[i + 1] + recvBufTime[i + 2];
		}

		timings << "total timings:" << endl;
		timings << "\treadData: " << totalData << endl;
		timings << "\tprocessData: " << totalProcess << endl;
		timings << "\treduceResults: " << totalReduce << endl;
		timings << "\ttotal: " << total << endl;

		timings.close();
	}
	
	MPI_Finalize();
	return 0;
}