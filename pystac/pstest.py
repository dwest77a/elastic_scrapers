import numpy as np

xarr = [[[-1.867061, 52.701756], [-1.8668937, 52.6982185], [-1.8668405, 52.694662], [-1.8668136, 52.6910854], [-1.8667338, 52.687501], [-1.8665599, 52.6839184], [-1.8663083, 52.680361], [-1.8660748, 52.6768467], [-1.8660661, 52.6733441], [-1.8662209, 52.6698132], [-1.8664225, 52.6662149], [-1.8665084, 52.662539], [-1.8665101, 52.6588204], [-1.8664767, 52.6551174], [-1.8664418, 52.6514687], [-1.8664412, 52.6478932], [-1.8665774, 52.6443939], [-1.8667382, 52.6409807], [-1.8668896, 52.6376183], [-1.8669325, 52.6342818], [-1.8669532, 52.630937], [-1.866975, 52.6275274], [-1.8668828, 52.6240694], [-1.8667225, 52.6205941], [-1.8666852, 52.6171193], [-1.8668088, 52.6136703], [-1.866993, 52.6102516], [-1.86718, 52.606837], [-1.8672946, 52.6034046], [-1.8673721, 52.6000154]], [[-1.8547066, 52.6012475], [-1.8551059, 52.604738], [-1.8553245, 52.6081831], [-1.8551722, 52.6115865], [-1.8550745, 52.6149936], [-1.8550312, 52.6183994], [-1.8549386, 52.6218021], [-1.8549126, 52.6252424], [-1.8550195, 52.6287614], [-1.8552312, 52.6323438], [-1.8553695, 52.6359116], [-1.8554456, 52.6394624], [-1.8554212, 52.643002], [-1.8552895, 52.6465407], [-1.8551283, 52.6500589], [-1.8550749, 52.6535233], [-1.8551765, 52.6569507], [-1.8552556, 52.6603605], [-1.8553492, 52.6638055], [-1.8554986, 52.667303], [-1.855706, 52.6708031], [-1.8559015, 52.6742655], [-1.8561181, 52.6776959], [-1.8563715, 52.6811031], [-1.8566532, 52.684501], [-1.8568514, 52.6879058], [-1.8569917, 52.6914303], [-1.8571038, 52.6951339], [-1.8572254, 52.6989219], [-1.8579685, 52.702702]], [[-1.8431522, 52.6724167], [-1.8431985, 52.6701505], [-1.8432214, 52.6678878], [-1.8431834, 52.6656196], [-1.8431463, 52.6633432], [-1.8430938, 52.6610497], [-1.8430963, 52.6587398], [-1.8432188, 52.656422], [-1.8433842, 52.6541031], [-1.8435517, 52.6517681], [-1.8436396, 52.6494138], [-1.8436489, 52.647054], [-1.8435713, 52.6447013], [-1.8434558, 52.6423609], [-1.8433357, 52.640029], [-1.8432406, 52.6377112], [-1.8431463, 52.6354102], [-1.8431208, 52.6331085], [-1.8431568, 52.6308019], [-1.8431921, 52.6284947], [-1.8432391, 52.6261956], [-1.8433046, 52.6239131], [-1.8433441, 52.6216551], [-1.8433731, 52.6194059], [-1.8433525, 52.6171558], [-1.8432851, 52.6148968], [-1.8431821, 52.6126301], [-1.8430235, 52.6103456], [-1.8428289, 52.6080301]], [[-1.8309722, 52.6956911], [-1.8307744, 52.6923799], [-1.8306913, 52.6890496], [-1.8308072, 52.6856988], [-1.8308738, 52.6823534], [-1.8308252, 52.679028], [-1.8307688, 52.6757194], [-1.8308483, 52.6724193], [-1.8311214, 52.6691225], [-1.8315276, 52.6658331], [-1.8317434, 52.6625292], [-1.8316536, 52.6592109], [-1.8315483, 52.6558665], [-1.8315654, 52.6524894], [-1.8316793, 52.649108], [-1.8315538, 52.6457259], [-1.8311551, 52.6423256], [-1.830822, 52.6389058], [-1.8307267, 52.6354889], [-1.8307337, 52.6320864], [-1.830798, 52.6286977], [-1.830896, 52.6253118], [-1.8310859, 52.6219278], [-1.8312693, 52.6185304], [-1.831321, 52.6151369], [-1.8312319, 52.6117498], [-1.8309864, 52.6083441], [-1.8306872, 52.6049068], [-1.8302558, 52.6014754]], [[-1.8130402, 52.5872085], [-1.807441, 52.5881819], [-1.8033496, 52.5906596], [-1.8019475, 52.594123], [-1.8034467, 52.5976995], [-1.8058276, 52.6010117], [-1.807107, 52.6043442], [-1.807776, 52.6077189], [-1.8080236, 52.6110419], [-1.8085082, 52.6142722], [-1.8088685, 52.617493], [-1.8088419, 52.6207772], [-1.8087931, 52.6241106], [-1.8088845, 52.6275211], [-1.8090535, 52.6310071], [-1.80924, 52.6345132], [-1.8094533, 52.6380042], [-1.8095102, 52.6414636], [-1.8094181, 52.6449244], [-1.808993, 52.648388], [-1.8085553, 52.6518425], [-1.8083113, 52.6553382], [-1.8083158, 52.6588896], [-1.8084596, 52.6624483], [-1.8084026, 52.6659582], [-1.8082304, 52.669406], [-1.8080029, 52.6728048], [-1.8078127, 52.6761784], [-1.8078206, 52.6796046]], [[-1.8197691, 52.6934234], [-1.8194487, 52.6901063], [-1.8192537, 52.6867383], [-1.8193819, 52.6833258], [-1.8195577, 52.6798917], [-1.819639, 52.676449], [-1.8195501, 52.6730058], [-1.8193481, 52.6695471], [-1.8192362, 52.6660914], [-1.8192946, 52.6626595], [-1.8195084, 52.6592581], [-1.8197702, 52.6558902], [-1.8198529, 52.6525444], [-1.8196754, 52.6491828], [-1.8195761, 52.6457669], [-1.8197009, 52.6422923], [-1.819874, 52.6387859], [-1.8198247, 52.6352745], [-1.8196964, 52.6317501], [-1.8197109, 52.6282077], [-1.8198362, 52.6247003], [-1.8196484, 52.6212438], [-1.8193352, 52.617823], [-1.8191997, 52.6144143], [-1.8194136, 52.6109845], [-1.8197064, 52.6075306], [-1.8200305, 52.6041061], [-1.820366, 52.6007124], [-1.8206605, 52.5973154]], [[-1.7977866, 52.590606], [-1.7981882, 52.5939747], [-1.79836, 52.5972983], [-1.7982223, 52.6005807], [-1.7978696, 52.603839], [-1.7975915, 52.6071238], [-1.797339, 52.6104315], [-1.7969238, 52.6137498], [-1.7965942, 52.617092], [-1.7964994, 52.6204345], [-1.7966403, 52.6237833], [-1.7966435, 52.6271539], [-1.7962409, 52.6305378], [-1.7960255, 52.6339628], [-1.7961444, 52.6374406], [-1.7963961, 52.6409371], [-1.7965652, 52.6444281], [-1.796517, 52.6479054], [-1.7965041, 52.651388], [-1.796672, 52.6548845], [-1.7968341, 52.6583968], [-1.7967754, 52.661903], [-1.7967481, 52.6653782], [-1.7969843, 52.6688198], [-1.7974127, 52.6722299], [-1.797999, 52.6756378], [-1.7987164, 52.6790658], [-1.7995572, 52.682505], [-1.8003866, 52.6859383]]]
print(xarr)
print(np.array(xarr[0]))
