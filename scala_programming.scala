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

// var x = 0
//
// while(x < 5){
//   println(s"x is currently $x")
//   println("x is still less than 5, adding 1 to x")
//   x = x+1
// }
//
// // Break statement //
// import util.control.Breaks._
//
// var y = 0
//
// while(y < 10){
//   println(s"y is currently $y")
//   println("y is still less than 10, add 1 to y")
//   y = y+1
//   if(y==3) break
// }

///////////////
// Functions //
///////////////

// def adder(num1: Int, num2: Int): Int = {
//   return num1 + num2
// }
//
// adder(4,5)

// def greetName(name: String): String = {
//   return s"Hello $name"
// }
//
// val fullgreet = greetName("Brian")
//
// println(fullgreet)

// def isPrime(numcheck: Int): Boolean = {
//   for(n <- Range(2, numcheck)){
//     if(numcheck % n == 0){
//       return false
//     }
//   }
//   return true
// }
//
// println(isPrime(10)) //false
// println(isPrime(23)) //true

// Using collections with Functions

// val numbers = List(1,2,3,7)
//
// def check(nums:List[Int]): List[Int]={
//   return nums
// }
//
// println(check(numbers))

///////////////
// Exercises //
///////////////

//1. Check for single even:
// Write a function that takes in an integer and returns a Boolean indicating
// whether or not it is even. Try to write it in one line

//def even(num:Int): Boolean={return num%2 == 0}
//even(2)
//OR
// def even(num:Int) = num % 2 == 0
// even(4)

//2. Check for evens in a list:
// Write a function that returns true if there is an even number inside a list,
// otherwise return fasle
// def evenList(list:List[Int]): Boolean = {
//   for(n<- list){
//     if (n%2==0){
//       return true
//     }
//   }
//   return false
// }
// val evensample = List(1,2,3,4,5)
// evenList(evensample)
// val oddsample = List(1,3,5)
// evenList(oddsample)

//3. Lucky number 7:
// take a list of integers and calculate their sum
// but sevens are lucky and they should be counted twice, meaning their value is 14 for the sum.
// assume the list isn't empty

// def sumlist(nums:List[Int]): Int = {
//   var output = 0
//   for(n <- nums){
//     if(n==7){
//       output = output + 14
//     }else{
//       output = output + n
//     }
//   }
//   return output
// }
//
// val numbers = List(1,2,3,7)
// println(sumlist(numbers))

//4. Can you balance?
// given a non-empty list of integers, return true if there is a place to split the List
// so that the sum of the numbers on one side is equal to the sum of the numbers on the other inside
// for example, given the list (1,5,3,3,) would return true , you can split in the middle.
// Another would be (7,3,4). You just need to return the boolean, not the split index point

// def balanceCheck(mylist:List[Int]): Boolean = {
//   var firsthalf = 0
//   var secondhalf = 0
//
//   secondhalf = mylist.sum
//
//   for(i <- Range(0, mylist.length)){
//     firsthalf = firsthalf + mylist(i)
//     secondhalf = secondhalf - mylist(i)
//
//     if(firsthalf == secondhalf){
//       return true
//     }
//   }
//   return false
// }
//
// val ballist = List(1,2,3,4,10)
// val ballist2 = List(2,3,3,2)
// var unballist = List(10,20,70)
//
// println(balanceCheck(ballist))
// println(balanceCheck(ballist2))
// println(balanceCheck(unballist))

//5. Palindrome Check
// Given a String, return a boolean indicator whether or not it is a Palindrome
// Try exploring methods to help you

//use the .reverse method (press tab to explore)

def palindromeCheck(st:String):Boolean = {
  return (st == st.reverse)
}
palindromeCheck("Lion")
palindromeCheck("ABBA")
