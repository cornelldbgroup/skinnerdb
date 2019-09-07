def monet():
    results = []
    with open("./imdb.txt") as f:
        for line in f:
            if line.startswith("clk"):
                clk = line.split(":")[1].split(" ")[0]
                results.append(float(clk))

    agg = sum(results) / 1000
    print(agg)
monet()