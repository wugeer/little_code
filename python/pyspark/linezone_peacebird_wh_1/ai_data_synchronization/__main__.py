import sys
from ai_data_synchronization.ai_data_synchronization.ai_data_synchronization import main
from ai_data_synchronization.ai_data_synchronization.ai_data_sales_slip import run

if sys.argv[2] == 'rst_sku_replenish_day_drp':
    main()
elif sys.argv[2] == 'rst_sku_sales_slip_day_drp':
    run()
else:
    print('输入参数表名错误！')
    # main()
    # run()
