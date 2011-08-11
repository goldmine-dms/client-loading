% Import of the Mauna Loa dataset

addpath '../client/matlab'

g = goldmine('http://localhost:8080/service');
g.authenticate('admin', 'password');

study = g.call('study.new', {'Mauna Loa', 'From NOAA'})
study = study.id;

x_type = '008d31d6-d01d-4994-a721-c7d5c9c5cab3'; % dateyear
y_type = '7d869ced-aa06-424d-998b-8dde2ba3adb6'; % atmospheric co_2

%%

load mauna_loa_co2.txt
x = mauna_loa_co2(:,3);
y1 = mauna_loa_co2(:,4);
y2 = mauna_loa_co2(:,5);

%%

dataset = g.call('dataset.new', {study, x_type, {y_type}, 'point', 'center', 'Original record from NOAA.gov', NaN});

param = struct();
setjsonfield(param, dataset.params{1}.id, y1);
g.call('dataset.append', {dataset.id, x, param});
g.call('dataset.close', {dataset.id});


%%

dataset2 = g.call('dataset.new', {study, x_type, {y_type}, 'point', 'center', 'Interpolated record', dataset.id});

param = struct();
setjsonfield(param, dataset2.params{1}.id, y2);
g.call('dataset.append', {dataset2.id, x, param});
g.call('dataset.close', {dataset2.id});
