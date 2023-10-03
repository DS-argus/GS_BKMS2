import os
import math
import sys
import time


RAM_SIZE = 10000 # number of lines : 100Byte * 10000 = 1MB


def split_file(input_file_name):

    # run file 개수, io 횟수
    run, io = 0, 0

    # input file을 열어서 읽음
    with open(input_file_name, 'r') as input_file:

        while True:
            lines = input_file.readlines(RAM_SIZE*101-1)    # RAM_SIZE만큼 읽음
            if not lines: break

            lines.sort()                                    # 정렬
            io += len(lines)                                # disk io 횟수 +

            # disk 폴더에 "pass0_R1.txt"와 같이 기록
            with open(f"./disk/pass0_R{run+1}.txt", 'w') as diskFile:
                diskFile.writelines(lines)
                io += len(lines)                            # disk io 횟수 +
            
            run += 1                                        # run file 개수 +1

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
        io, start = 0, 0

        # PASS 시작
        for run in range(run_num):
            
            # 이번 run에서 읽어야할 diskfile
            disk_files = total_disk_files[start:start+BUFFER_NUM]
            disk_files_cursor = [0 for _ in range(len(disk_files))]
            
            # disk file과 cursor 정보를 모두 담고 있는 dictionary
            disk_cursor_info = dict(zip(disk_files, disk_files_cursor))
            
            buffers = [[] for _ in range(BUFFER_NUM)]
            disk_buffer_info = dict(zip(disk_files, buffers))

            # 이번에 읽어야하는 disk_files
            active_disk_files = disk_files

            # 다 읽은 file 저장
            completed_disk_files = []

            # 새로운 run file 작성 시작
            with open(f"./disk/pass{pass_cnt+1}_R{run+1}.txt", 'w') as newDiskFile:

                while True:

                    # 읽어야하는 diskfile 마다 buffer size만큼의 line을 읽어서 buffer에 저장
                    for disk_file in active_disk_files:
                        if disk_file not in completed_disk_files:
                            cursor = disk_cursor_info[disk_file]

                            # run에서 buffer_size만큼 line을 읽어서 buffers에 저장
                            with open(f"./disk/{disk_file}", 'r') as file:
                            
                                # 이전까지 읽었던 위치 찾기 : 몇번째 읽는 건지 * 한줄의 길이(101)* 한번에 읽는 line 수(buffer_size*10)
                                file.seek(cursor*101*BUFFER_SIZE*10, 0)
                                
                                # BUFFER_SIZE에 해당하는 line만큼 읽기
                                lines = file.readlines(BUFFER_SIZE*10*101-1)

                                # 만약 다 run file을 읽었다면 완료 목록에 추가
                                if not lines:
                                    completed_disk_files.append(disk_file)
                                    continue

                                disk_buffer_info[disk_file] += lines
                                io += len(lines)

                                disk_cursor_info[disk_file] += 1
                    
                    # 모든 run file을 다 읽었으면 종료
                    if len(completed_disk_files) == len(disk_files):
                        break

                    #데이터 sorting   
                    while True:
                        min_value = "A"
                        for file, buffer in disk_buffer_info.items():
                            
                            if file not in completed_disk_files:
                                min_value = min(min_value, buffer[0])
                                if min_value == buffer[0]:
                                    min_buffer = file


                        newDiskFile.write(min_value)
                        io += 1

                        disk_buffer_info[min_buffer].remove(min_value)

                        # 만약 이번에 가장 작은걸 제거한 buffer가 0이 되면, active_disk_files에 넣어서 buffer에 추가함
                        if len(disk_buffer_info[min_buffer]) == 0:
                            active_disk_files = [min_buffer]
                            break

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
    split_file("./input_file.txt")
    
    # 2. Merge phase
    external_merge_sort()

    # measure end time 
    end_time = time.time()
    
    # print statistics
    print_time_statistics(start_time, end_time)
