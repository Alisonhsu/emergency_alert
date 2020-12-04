def find_largest_digit(n):
    
    # 利用遞迴找出數字最大的一個值
    def number(digit, n):   
        if n == 0:
            return digit 
        else:
            
            # max():找出最大值; n%10找餘數(n=1~9); n//10只留下整數部分 --> 0
            return number(max(digit, n % 10), n // 10) 
        
    # abs():取絕對值    
    return number(0, abs(n)) 
main()
