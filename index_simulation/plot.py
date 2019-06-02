import matplotlib.pyplot as plt

buffers = [3, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000]

plt.figure()
page = [1062500, 437500, 312500, 250000, 250000, 250000, 187500, 187500, 187500, 187500]
page_rw = [p * 2 for p in page]
# plot
title = 'page read and write with page size {}'.format(512)
plt.title(title)
plt.plot([str(b) for b in buffers], page_rw)
plt.xlabel("buffer size")
plt.ylabel("page read and write")
plt.savefig('ex_sort_{}.png'.format(512))

plt.figure()
page = [500000, 187500, 156250, 125000, 125000, 93750, 93750, 93750, 93750, 93750]
page_rw = [p * 2 for p in page]
# plot
title = 'page read and write with page size {}'.format(1024)
plt.title(title)
plt.plot([str(b) for b in buffers], page_rw)
plt.xlabel("buffer size")
plt.ylabel("page read and write")
plt.savefig('ex_sort_{}.png'.format(1024))

plt.figure()
page = [234375, 93750, 78125, 62500, 62500, 46875, 46875, 46875, 46875, 46875]
page_rw = [p * 2 for p in page]
# plot
title = 'page read and write with page size {}'.format(2048)
plt.title(title)
plt.plot([str(b) for b in buffers], page_rw)
plt.xlabel("buffer size")
plt.ylabel("page read and write")
plt.savefig('ex_sort_{}.png'.format(2048))