package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

import (
	_ "github.com/go-sql-driver/mysql"
)

type datapoint struct {
	x int
	y int
	iterations int
}

var waitGroup sync.WaitGroup
var db *sql.DB

func main() {
	fmt.Printf("%s\n", "Mandelbrot Generator")

	var err error
	db, err = sql.Open("mysql", "mandelbrot:password@tcp(127.0.0.1:3306)/mandelbrot")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()


	var px_x = 1920*10
	var px_y = 1080*10

	//var max_iteration = 16777215 //max max_iterations value the db can handle
	var max_iteration = 10000

	var mutex = &sync.Mutex{}
	throttle := make(chan struct{}, 32)

	for px_xi := 0; px_xi < px_x; px_xi++ {
		waitGroup.Add(1)
		throttle <- struct{}{}

		go func(px_xi int) {
			defer waitGroup.Done()
			defer func() {<-throttle}()

			var rows []datapoint
			for px_yi := 0; px_yi < px_y; px_yi++ {
				x0 := scale(px_xi, 0, px_x, -2.5, 1)
				//x0 := scale(px_xi, 0, px_x, -0.035, 0.025)
				y0 := scale(px_yi, 0, px_y, -1, 1)
				//y0 := scale(px_yi, 0, px_y, 0.73, 0.77)

				iterations := calc_iterations(x0, y0, max_iteration)

				if iterations != max_iteration {
					dp := datapoint{x: px_xi, y: px_yi, iterations: iterations}
					rows = append(rows, dp)
				}

				if (len(rows) >= 10000) {
					mutex.Lock()
					fmt.Printf("Starting insert for row: %d\n", px_xi)
					err = bulkInsert(rows)
					if err != nil {
						fmt.Printf("%s\n", err)
					}
					rows = nil
					fmt.Printf("Finished insert\n")
					mutex.Unlock()
				}
			}

			if rows != nil {
				mutex.Lock()
				fmt.Printf("Starting insert for row: %d\n", px_xi)
				err = bulkInsert(rows)
				if err != nil {
					fmt.Printf("%s\n", err)
				}
				rows = nil
				fmt.Printf("Finished insert\n")
				mutex.Unlock()
			}
		}(px_xi)
		fmt.Printf("Finished Column: %d\n", px_xi+1)
	}

	waitGroup.Wait()
}

func bulkInsert(rows []datapoint) (err error) {
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows) * 3)

	for _, dp := range rows {
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, dp.x)
		valueArgs = append(valueArgs, dp.y)
		valueArgs = append(valueArgs, dp.iterations)
	}

	stmt := fmt.Sprintf("INSERT INTO mandelbrot (x, y, iterations) VALUES %s", strings.Join(valueStrings, ","))
	_, err = db.Exec(stmt, valueArgs...)
	return err
}

func scale(input int, input_min int, input_max int, output_min float64, output_max float64) (output float64) {
	output = (float64(input) - float64(input_min)) / (float64(input_max) - float64(input_min)) * (output_max - output_min) + output_min
	return output
}

func calc_iterations (x0 float64, y0 float64, max_iteration int) (iterations int) {
	var x float64
	var y float64

	for x * x + y * y <= 2*2 && iterations < max_iteration {
		xtemp := x * x - y * y + x0
		y = 2 * x * y + y0
		x = xtemp
		iterations++
	}
	return iterations
}
