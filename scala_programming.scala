//Programmer: Brian Choi
//Date: 4.4.2020
//Basic Scala Programming Notes

///////////////////
// FLOW CONTROL ///
///////////////////

// val person = "Jose"
//
// if(person == "Sammy"){
//   println("welcome Sammy")
// }else if(person == "George"){
//   println("welcome George!")
// }else{
//   println("what is your name?")
// }
//
// //AND
// println((1==1) && (2==2))
//
// //OR
// println((1==2) || (2==2))
//
// //NOT
// println(!(1==1))


/////////////
//For Loops//
/////////////

// for(item <- List(1,2,3)){
//   println(item)
// }
//
// for(num <- Set(1,2,3,4,5,6)){
//   println(num)
// }

// for(num <- Range(0,10)){
//   if(num%2 == 0){
//     println(s"$num is even")
//   }else{
//     println(s"$num is odd")
//   }
// }

// val names = List("John", "abe", "Shangols", "Jangols")
// for(name <- names){
//   if(name.startsWith("J")){
//     println(s"$name starts with a J")
//   }
// }


///////////////
//WHILE Loops//
///////////////

var x = 0

while(x < 5){
  println(s"x is currently $x")
  println("x is still less than 5, adding 1 to x")
  x = x+1
}

// Break statement //
import util.control.Breaks._

var y = 0

while(y < 10){
  println(s"y is currently $y")
  println("y is still less than 10, add 1 to y")
  y = y+1
  if(y==3) break
}
