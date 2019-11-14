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
var db sql.DB

func main() {
	fmt.Printf("%s\n", "Mandelbrot Generator")

	db, err := sql.Open("mysql", "mandelbrot:password@tcp(127.0.0.1:3306)/mandelbrot")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//stmt, err := db.Prepare("INSERT INTO mandelbrot(x, y, iterations) VALUES (?,?,?)")


	var px_x = 1920*2
	var px_y = 1080*2

	var max_iteration = 1000

	var mutex = &sync.Mutex{}

	//image := image2.NewNRGBA(image2.Rectangle{image2.Point{0,0}, image2.Point{px_x, px_y}})

	for px_xi := 0; px_xi < px_x; px_xi++ {
		waitGroup.Add(1)
		go func(px_xi int) {
			defer waitGroup.Done()

			var rows []datapoint
			for px_yi := 0; px_yi < px_y; px_yi++ {
				//x0 := float64(px_xi) / float64(px_x) * 3.5 -2.5
				//x0 := scale(px_xi, 0, px_x, -2.5, 1)
				x0 := scale(px_xi, 0, px_x, -0.035, 0.025)
				//y0 := float64(px_yi) / float64(px_y) * 2 - 1
				//y0 := scale(px_yi, 0, px_y, -1, 1)
				y0 := scale(px_yi, 0, px_y, 0.73, 0.77)

				iterations := calc_iterations(x0, y0, max_iteration)

				dp := datapoint{x: px_xi, y: px_yi, iterations: iterations}
				rows = append(rows, dp)

				if (len(rows) >= 1000) {
					mutex.Lock()
					err = bulkInsert(rows)
					if err != nil {
						fmt.Printf("%s\n", err)
					}
					rows = nil
					mutex.Unlock()
				}

				/*_, err := stmt.Exec(px_xi, px_yi, iterations)
				if err != nil {
					fmt.Printf("%s\n", err)
				}*/




				/*iteration_scaled := uint8(scale(iteration, 0, max_iteration, 0, 255))

				if iteration < 1000 {
					image.Set(px_xi, px_yi, color.RGBA{R:iteration_scaled, G:0, B: 0, A: 255})
				}
				if (iteration < 666) {
					image.Set(px_xi, px_yi, color.RGBA{R:iteration_scaled, G:iteration_scaled, B: 0, A: 255})
				}
				if (iteration < 333) {
					image.Set(px_xi, px_yi, color.RGBA{R:iteration_scaled, G:iteration_scaled, B: iteration_scaled, A: 255})
				}*/

			}
		}(px_xi)
		fmt.Printf("Finished Column: %d\n", px_xi+1)
	}

	waitGroup.Wait()

	//output, _ := os.Create("mandelbrot.png")
	//png.Encode(output, image)
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
	fmt.Printf("%d\n", len(rows))
	fmt.Printf("%d\n", len(valueArgs))
	fmt.Printf("%+v\n", valueArgs)
	fmt.Printf("%+v\n", valueStrings)
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
