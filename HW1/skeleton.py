import os
import math
import sys
import time
import heapq


RAM_SIZE = 10000 # number of lines : 100Byte * 10000 = 1MB


def split_file(input_file_name):

    # run file 개수, io 횟수, 읽은 line 초기화
    run, io, l = 0, 0, 0

    # 정렬을 위해 min heap 사용
    min_heap = []

    # input file을 열어서 읽음
    with open(input_file_name, 'r') as input_file:

        # 아래 과정 반복
        while True:

            line = input_file.readline()                # 한 줄 읽음
            if not line: break                          # 만약 다 읽었다면, 종료

            key = line[:10]                             # 정렬 기준인 key 할당 
            heapq.heappush(min_heap, (key, line))       # min heap에 데이터 추가
            l += 1                                      # 읽은 line +1
            io += 1                                     # disk io 횟수 +1

            # 만약 하나의 run file 크기만큼 읽었으면 run file에 데이터 write
            if l%RAM_SIZE == 0:                         
                
                # disk 폴더에 "pass0_R1.txt"와 같이 기록
                with open(f"./disk/pass0_R{run+1}.txt", 'w') as diskFile:

                    # min_heap에 있는 line을 diskFile에 write
                    while min_heap:
                        key, line = heapq.heappop(min_heap)
                        diskFile.write(line)
                        io += 1                         # disk io 횟수 +1
                
                run += 1                                # run file 개수 +1

    # 통계량 출력            
    print_pass_statistics(0, run, io)
    
    
def external_merge_sort(): 
    
    # 편의를 위한 변수 할당 및 pass_cnt 초기화
    BUFFER_SIZE = n                                     
    BUFFER_NUM = int(RAM_SIZE / (BUFFER_SIZE*10))       
    pass_cnt = 0

    # 최종 run file이 만들어질 때까지 반복
    while True:
        
        # PASS를 시작하기 전에 알아야하는 정보들
        # 1. 현재 disk에 있는 이전 PASS에서 생성된 run file list
        total_disk_files = os.listdir("./disk")           
        total_disk_files = [file for file in total_disk_files if f"pass{pass_cnt}" in file]
        total_disk_files.sort()

        # 만약 이전 PASS로 생성된 run이 1개면 break
        if len(total_disk_files) == 1: break

        # 2. 만들어야하는 run file 개수 : buffer 개수가 더 많은 경우 1개만 생성됨
        run_num = math.ceil(len(total_disk_files) / BUFFER_NUM)

        # 3. 각 pass마다 initializing해야하는 것들
        #   io : disk read/write count
        #   start : 하나의 run을 만들기 위해 몇번째 disk file부터 읽어야 하는지 체크
        io, start = 0, 0

        # PASS 시작
        for run in range(run_num):
            
            # 하나의 run을 만드는 과정
            # 이번 run에서 읽어야할 diskfile
            disk_files = total_disk_files[start:start+BUFFER_NUM]
            
            original_files, read_count, flag_count= len(disk_files), 0, 0

            # 만들어야 하는 run file 수 만큼 iterate
            while True:

                min_heap = []

                # 읽어야하는 diskfile 마다 buffer size만큼의 line을 읽어서 sort
                for disk_file in disk_files:
                    
                    # run에서 buffer_size만큼 line을 읽어서 buffers에 저장
                    with open(f"./disk/{disk_file}", 'r') as file:
                        # 이전까지 읽었던 위치 찾기 : 몇번째 읽는 건지 * 한줄의 길이(101)* 한번에 읽는 line 수(buffer_size*10)
                        file.seek(read_count*101*BUFFER_SIZE*10, 0)
                        
                        # BUFFER_SIZE에 해당하는 line만큼 읽기
                        for _ in range(BUFFER_SIZE*10):
                            line = file.readline()

                            # 만약 run file의 크기가 작은 파일이 있으면 나머지 파일만 계속 읽어야 함
                            if not line: 
                                flag_count += 1
                                disk_files.remove(disk_file)
                                break

                            key = line[:10]
                            heapq.heappush(min_heap, (key, line))
                            io += 1
                    
                    # 모든 파일을 다 읽으면 종료
                    if flag_count == original_files: break

                # 모든 파일을 다 읽으면 종료
                if flag_count == original_files: break    

                #sort된 데이터를 run 파일에 기록 (이어서)   
                with open(f"./disk/pass{pass_cnt+1}_R{run+1}.txt", 'a') as diskFile:
                    while min_heap:
                        key, line = heapq.heappop(min_heap)
                        diskFile.write(line)
                        io += 1
                
                read_count += 1

            #run 종료
            start += BUFFER_NUM
            
        # PASS 종료
        pass_cnt += 1
        print_pass_statistics(pass_cnt, run_num, io)


def print_pass_statistics(n, run, io):
    print('[PASS {0}]'.format(n))
    print('  > # of Generated Runs: {0}'.format(run) )
    print('  > # of IOs: {0}'.format(io) , end="\n\n" )


def print_time_statistics(start_time, end_time):
    print("==============================")
    print(f"Execution Time : {end_time - start_time:.5f} sec")


if __name__ == '__main__':
    
    assert (len(sys.argv)==2), 'Missing Argument. $ python3 ./skeleton.py [n]'
    
    # argument parsing
    n = int(sys.argv[1]) # size of buffer
    
    # remove files in disk folder
    for file in os.listdir("./disk"):
        os.remove(f"./disk/{file}")

    # measure start time
    start_time = time.time()
    
    # 1. split phase
    split_file("input_file.txt")
    
    # 2. Merge phase
    external_merge_sort()

    # measure end time 
    end_time = time.time()
    
    # print statistics
    print_time_statistics(start_time, end_time)
