import pandas as pd

# Data for all rows
data = [
    (1, '2009-04-15', '13:44:00', 26.162202, 119.943787, 0.0, 3.722979540040798, False, False, False),
    (2, '2009-04-15', '13:44:05', 26.161528, 119.943234, 18.61490673250718, -2.068227268123859, False, False, False),
    (3, '2009-04-15', '13:44:10', 26.1619, 119.943228, 8.273765374171417, -1.0666832133399906, False, True, False),
    (4, '2009-04-15', '13:44:15', 26.161775, 119.943276, 2.940356107473505, -0.10729482871178968, False, True, False),
    (5, '2009-04-15', '13:44:20', 26.161667, 119.943281, 2.4038816361569615, 0.1301658537706168, False, False, False),
    (6, '2009-04-15', '13:44:25', 26.16167, 119.943128, 3.0547112208048124, 2.521308459821934, True, False, False),
    (7, '2009-04-15', '13:44:30', 26.161566, 119.942352, 15.661237446817559, -2.933421091139979, False, False, False),
    (8, '2009-04-15', '13:44:35', 26.161558, 119.942401, 0.9941230302846551, 0.25539882188457097, False, False, False),
    (9, '2009-04-15', '13:44:40', 26.161482, 119.942477, 2.271117759329393, -0.34537514691089627, False, False, False),
    (10, '2009-04-15', '13:44:45', 26.161461, 119.942491, 0.5442411868618886, 0.09912297752006315, False, False, False),
    (11, '2009-04-15', '13:44:50', 26.16143, 119.94253, 1.0398554425628352, -0.0920794684995707, False, False, False),
    (12, '2009-04-15', '13:44:55', 26.161413, 119.942552, 0.5794578187863262, -0.09403833460265562, False, False, False),
    (13, '2009-04-15', '13:45:00', 26.161411, 119.942557, 0.1092659176270858, 0.002156777042684049, False, False, False),
    (14, '2009-04-15', '13:45:05', 26.161408, 119.942562, 0.12004978909126156, -0.006080071486473824, False, False, True),
    (15, '2009-04-15', '13:45:10', 26.161405, 119.942565, 0.08964941308585678, 0.002520373929607384, False, False, True),
    (16, '2009-04-15', '13:45:15', 26.161404, 119.94257, 0.10225128884856227, -0.003873798815516996, False, False, True),
    (17, '2009-04-15', '13:45:20', 26.161403, 119.942574, 0.08288231946609557, -0.001657804152259608, False, False, True),
    (18, '2009-04-15', '13:45:25', 26.161401, 119.942577, 0.0745932946828104, -0.00894202578669897, False, False, True),
    (19, '2009-04-15', '13:45:30', 26.1614, 119.942578, 0.029883138433783, 6.959163650646133e-07, False, False, True),
    (20, '2009-04-15', '13:45:35', 26.161399, 119.942579, 0.029883141913366513, -0.00152882759181411, False, False, True),
    (21, '2009-04-15', '13:45:40', 26.161398, 119.942579, 0.0222390137004236, 0.0015288241280204826, False, False, True),
    (22, '2009-04-15', '13:45:45', 26.161397, 119.94258, 0.0298831390106838, -0.0015288321492590747, False, False, True),
    (23, '2009-04-15', '13:45:50', 26.161396, 119.94258, 0.022238974555338107, -4.55645154709821e-07, False, False, True),
    (24, '2009-04-15', '13:45:55', 26.161396, 119.942581, 0.019960751686482675, 0.001984477241768231, False, False, True),
    (25, '2009-04-15', '13:46:00', 26.161395, 119.942582, 0.029883142709854753, -0.005976624890564468, False, False, True),
    (26, '2009-04-15', '13:46:05', 26.161395, 119.942582, 0.0, 0.004447792738767348, False, False, True),
    (27, '2009-04-15', '13:46:10', 26.161394, 119.942582, 0.022238974484605833, 7.843173553239072e-09, False, False, True),
    (28, '2009-04-15', '13:46:15', 26.161393, 119.942582, 0.0222390137004236, -0.007413009047446769, False, False, True),
    (29, '2009-04-15', '13:46:18', 26.161393, 119.942582, 0.0, 0.03735376510160567, False, False, True),
    (30, '2009-04-15', '13:46:20', 26.161392, 119.942583, 0.07470771204220264, -1.2699692132131553e-06, False, False, True),
    (31, '2009-04-15', '15:17:30', 26.161155, 119.94247, 0.005240396063239276, 0.23943198792426315, False, False, True),
    (32, '2009-04-15', '15:17:35', 26.161101, 119.942473, 1.2023988093288513, -0.17598876474006434, False, False, False),
    (33, '2009-04-15', '15:17:40', 26.161099, 119.942489, 0.32245455866301337, 0.020111410518565298, False, False, False),
    (34, '2009-04-15', '15:17:45', 26.16108, 119.94249, 0.4230116726909307, -0.03587755962846707, False, False, True),
    (35, '2009-04-15', '15:17:50', 26.161082, 119.942502, 0.2436237875062163, 0.06361116294351242, False, False, True),
    (36, '2009-04-15', '15:17:55', 26.161107, 119.942498, 0.5616791967087834, -0.08094633711561441, False, False, False),
    (37, '2009-04-15', '15:18:00', 26.161114, 119.942497, 0.15694731474718276, -0.052315803203542725, False, False, False),
    (38, '2009-04-15', '15:18:03', 26.161114, 119.942497, 0.0, 0.15969552078205126, False, False, True),
    (39, '2009-04-15', '15:18:05', 26.161116, 119.942503, 0.31939181896558627, -0.02555123661162736, False, False, True),
    (40, '2009-04-15', '15:18:10', 26.161119, 119.942512, 0.19163579879410378, -0.06387845080993497, False, False, True),
    (41, '2009-04-15', '15:18:13', 26.161119, 119.942512, 0.0, 0.13006110835979606, False, False, True),
    (42, '2009-04-15', '15:18:15', 26.161122, 119.942516, 0.260121623430503, -0.011931355263531557, False, False, True),
    (43, '2009-04-15', '15:18:20', 26.161126, 119.942525, 0.20046481066568417, 0.013492912104389458, False, False, True),
    (44, '2009-04-15', '15:18:25', 26.161129, 119.942538, 0.2679294039227222, 0.126184325096181, False, False, True),
    (45, '2009-04-15', '15:18:30', 26.161162, 119.942564, 0.8988502249907917, 0.09136333253027253, False, False, False),
    (46, '2009-04-15', '15:18:35', 26.161222, 119.942576, 1.3556671092983066, -0.14562407128961244, False, False, False),
    (47, '2009-04-15', '15:18:40', 26.161243, 119.942555, 0.6275463080068536, 0.04665163737805299, False, False, False),
    (48, '2009-04-15', '15:18:45', 26.161255, 119.942514, 0.8608041974974544, -0.03311425239701968, False, False, False),
    (49, '2009-04-15', '15:18:50', 26.161265, 119.942481, 0.6952328551740237, -0.0368410925474835, False, False, False),
    (50, '2009-04-15', '15:18:55', 26.161257, 119.942457, 0.5110272798967191, 0.008745774726152013, False, False, False),
    (51, '2009-04-15', '15:19:00', 26.161278, 119.942442, 0.5547560977740135, -0.07782338506855753, False, False, False),
    (52, '2009-04-15', '15:19:05', 26.161274, 119.942435, 0.16563898362427656, -0.01715919804876968, False, False, False),
    (53, '2009-04-15', '15:19:10', 26.161274, 119.942431, 0.0798429517505783, -0.0031929076526047197, False, False, True),
    (54, '2009-04-15', '15:19:15', 26.161275, 119.942434, 0.06387840373404605, -0.0036360933495826267, False, False, True),
    (55, '2009-04-15', '15:19:20', 26.161274, 119.942432, 0.04569796016588715, 0.010821140552265537, False, False, True),
    (56, '2009-04-15', '15:19:25', 26.161274, 119.942427, 0.09980368918033314, -0.03326791650151629, False, False, True),
    (57, '2009-04-15', '15:19:28', 26.161274, 119.942427, 0.0, 0.1768540981707218, False, False, True),
    (58, '2009-04-15', '15:19:30', 26.161275, 119.942434, 0.35370905727127844, 0.04109642267572416, False, False, True),
    (59, '2009-04-15', '15:19:35', 26.1613, 119.942437, 0.5591909086641901, -0.06311348163897126, False, False, False),
    (60, '2009-04-15', '15:19:40', 26.161298, 119.942449, 0.24362334735001212, 0.16936340184057264, False, False, False),
    (61, '2009-04-15', '15:19:45', 26.161347, 119.942451, 1.0904407674446297, -0.17718775585083804, False, False, False),
    (62, '2009-04-15', '15:19:50', 26.161349, 119.942461, 0.20450300635740393, -0.06816752429176949, False, False, False),
    (63, '2009-04-15', '15:19:53', 26.161349, 119.942461, 0.0, 0.1596966435159151, False, False, True),
    (64, '2009-04-15', '15:19:55', 26.161351, 119.942467, 0.3193926589488865, 0.22001268248877243, True, False, True),
    (65, '2009-04-15', '15:20:00', 26.161408, 119.942499, 1.419456605164485, -0.025409601080789, False, False, False),
    (66, '2009-04-15', '15:20:05', 26.161458, 119.942532, 1.292408522140906, -0.15152707028785747, False, False, False),
    (67, '2009-04-15', '15:20:10', 26.161442, 119.942512, 0.5347741366719971, -0.09098621593784108, False, False, False),
    (68, '2009-04-15', '15:20:15', 26.161442, 119.942516, 0.0798428362415603, 0.009582760659633621, False, False, False),
    (69, '2009-04-15', '15:20:20', 26.16144, 119.94251, 0.12775666278841907, -0.02155920890367513, False, False, True),
    (70, '2009-04-15', '15:20:25', 26.16144, 119.942511, 0.019960742154869333, -0.0066535666147875455, False, False, True),
    (71, '2009-04-15', '15:20:28', 26.16144, 119.942511, 0.0, 0.09980398796609198, False, False, True),
    (72, '2009-04-15', '15:20:30', 26.16144, 119.942515, 0.19960758340557117, -0.003992237692252882, False, False, True),
    (73, '2009-04-15', '15:20:35', 26.16144, 119.942524, 0.17964638525876836, -0.01547901824262507, False, False, True),
    (74, '2009-04-15', '15:20:40', 26.161439, 119.942529, 0.10225124676129708, -0.008473813254823142, False, False, True),
    (75, '2009-04-15', '15:20:45', 26.161439, 119.942532, 0.05988223450692324, -0.002226127066067654, False, False, True),
    (76, '2009-04-15', '15:20:50', 26.161437, 119.942533, 0.048751593775812736, -2.6468762939125106e-06, False, False, True),
    (77, '2009-04-15', '15:20:55', 26.161435, 119.942534, 0.04875159364346886, -6.107381145781322e-07, False, False, True),
    (78, '2009-04-15', '15:21:00', 26.161434, 119.942536, 0.04569790658003837, 0.01641174442853499, False, False, True),
    (79, '2009-04-15', '15:21:05', 26.161436, 119.942542, 0.12775666853916945, -0.0425855819196562, False, False, True),
    (80, '2009-04-15', '15:21:08', 26.161436, 119.942542, 0.0, 0.05712207499779148, False, False, True),
    (81, '2009-04-15', '15:21:10', 26.161435, 119.942544, 0.11424439215778581, 0.02688052611248621, False, False, True),
    (82, '2009-04-15', '15:21:15', 26.161432, 119.942532, 0.2486468682577172, -0.04174506903656487, False, False, True),
    (83, '2009-04-15', '15:21:20', 26.161432, 119.94253, 0.039921421797386905, -0.003536487746580296, False, False, True),
    (84, '2009-04-15', '15:21:25', 26.161433, 119.94253, 0.022238974484605833, 0.01551290820190264, False, False, True),
    (85, '2009-04-15', '15:21:30', 26.161433, 119.942525, 0.09980355312991282, -0.03326787115134877, False, False, True),
    (86, '2009-04-15', '15:21:33', 26.161433, 119.942525, 0.0, 0.10360314998051662, False, False, True),
    (87, '2009-04-15', '15:21:35', 26.161434, 119.942529, 0.20720589249241034, 0.13385516804765304, True, False, True),
    (88, '2009-04-15', '15:21:40', 26.16146, 119.942562, 0.8764820574760706, -0.008819323942381166, False, False, False),
    (89, '2009-04-15', '15:21:45', 26.161484, 119.942594, 0.8323854163676044, -0.08660498572562476, False, False, False),
    (90, '2009-04-15', '15:21:50', 26.161499, 119.942583, 0.3993609853942706, 0.001270476523416352, False, False, False),
    (91, '2009-04-15', '15:21:55', 26.161489, 119.942566, 0.4057133710936521, -0.04024216694284255, False, False, True),
    (92, '2009-04-15', '15:22:00', 26.161487, 119.942556, 0.20450243874810697, 0.030990945404052582, False, False, True),
    (93, '2009-04-15', '15:22:05', 26.161501, 119.942565, 0.3594569876863311, -0.026702484740199133, False, False, True),
    (94, '2009-04-15', '15:22:10', 26.161511, 119.942567, 0.22594449920259918, 0.03497086203787107, False, False, True),
    (95, '2009-04-15', '15:22:15', 26.161529, 119.942568, 0.4007988942345988, -0.13359971216298663, False, False, True),
    (96, '2009-04-15', '15:22:18', 26.161529, 119.942568, 0.0, 0.1365817690024509, False, False, True),
    (97, '2009-04-15', '15:22:20', 26.161531, 119.942573, 0.2731641170269708, 0.05630978317249335, False, False, True),
    (98, '2009-04-15', '15:22:25', 26.161545, 119.94255, 0.5547127093187668, 0.039234126641938705, False, False, False),
    (99, '2009-04-15', '15:22:30', 26.161519, 119.942526, 0.7508834377141909, -0.2502946305232344, False, False, False),
    (100, '2009-04-15', '15:22:33', 26.161519, 119.942526, 0.0, 0.15006131283624194, False, False, False),
    (101, '2009-04-15', '15:22:35', 26.161516, 119.942521, 0.30012326183946625, -0.014128577639842354, False, False, True),
    (102, '2009-04-15', '15:22:40', 26.161513, 119.94251, 0.22948045482675025, -0.012768335531721263, False, False, True),
    (103, '2009-04-15', '15:22:45', 26.161509, 119.942503, 0.16563874619091376, -0.023377420175306738, False, False, True),
    (104, '2009-04-15', '15:22:50', 26.161507, 119.942502, 0.04875158859855069, 0.003025366565107454, False, False, True),
    (105, '2009-04-15', '15:22:55', 26.161506, 119.942505, 0.06387840403954421, -0.021292756212461395, False, False, True),
    (106, '2009-04-15', '15:22:58', 26.161506, 119.942505, 0.0, 0.1768553115870758, False, False, True),
    (107, '2009-04-15', '15:23:00', 26.161507, 119.942512, 0.35370992760659137, -0.02593650353455338, False, False, True),
    (108, '2009-04-15', '15:23:05', 26.161509, 119.942523, 0.22402734700939497, -0.028229034743255827, False, False, True),
    (109, '2009-04-15', '15:23:10', 26.161508, 119.942527, 0.08288210480677757, -0.027627384967703052, False, False, True),
    (110, '2009-04-15', '15:23:13', 26.161508, 119.942527, 0.0, 0.07984822609074484, False, False, True),
    (111, '2009-04-15', '15:23:15', 26.161509, 119.94253, 0.1596961381403941, 0.14925856756844377, False, False, True),
    (112, '2009-04-15', '15:23:20', 26.161499, 119.942574, 0.9059893380981232, -0.021432593590353138, False, False, False),
    (113, '2009-04-15', '15:23:25', 26.161464, 119.942565, 0.7988263181488191, 0.015699270229933164, False, False, False),
    (114, '2009-04-15', '15:23:30', 26.161439, 119.942531, 0.8773225790863919, -0.14613776435570341, False, False, False),
    (115, '2009-04-15', '15:23:35', 26.161437, 119.942524, 0.1466334027637305, -0.004964371171257027, False, False, False),
    (116, '2009-04-15', '15:23:40', 26.161436, 119.942518, 0.12181153486340691, -0.04060386949659885, False, False, True),
    (117, '2009-04-15', '15:23:43', 26.161436, 119.942518, 0.0, 0.0, False, False, True),
    (118, '2009-04-15', '15:23:45', 26.161436, 119.942518, 0.0, None, False, False, True)
]

# Create DataFrame
df = pd.DataFrame(data, columns=['id', 'date', 'time', 'latitude', 'longitude', 'speed', 'acceleration', 'is_accelerating', 'is_decelerating', 'is_stop'])

# Save to CSV
df.to_csv('Spark/data/output.csv', index=False,encoding='gbk')