data = 'data.csv';
m = csvread(data);
x = m(:, 1)
y = m(:, 2)
hold on;
grid on;
plot(x, y, 'o-r');
set(gca,'XTick',0:10:90)
xlabel("percentage dropped")
ylabel("time")
print -djpg image.jpg
