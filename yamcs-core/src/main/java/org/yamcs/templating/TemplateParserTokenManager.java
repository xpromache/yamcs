/* Generated By:JavaCC: Do not edit this line. TemplateParserTokenManager.java */
package org.yamcs.templating;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** Token Manager. */
public class TemplateParserTokenManager implements TemplateParserConstants
{

  /** Debug output. */
  public  java.io.PrintStream debugStream = System.out;
  /** Set debug output. */
  public  void setDebugStream(java.io.PrintStream ds) { debugStream = ds; }
private final int jjStopStringLiteralDfa_0(int pos, long active0)
{
   switch (pos)
   {
      case 0:
         if ((active0 & 0x302L) != 0L)
            return 4;
         return -1;
      case 1:
         if ((active0 & 0x302L) != 0L)
         {
            jjmatchedKind = 2;
            jjmatchedPos = 1;
            return 5;
         }
         return -1;
      case 2:
         if ((active0 & 0x302L) != 0L)
         {
            jjmatchedKind = 2;
            jjmatchedPos = 2;
            return -1;
         }
         return -1;
      case 3:
         if ((active0 & 0x302L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 4:
         if ((active0 & 0x302L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 5:
         if ((active0 & 0x302L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 6:
         if ((active0 & 0x202L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 7:
         if ((active0 & 0x202L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 8:
         if ((active0 & 0x2L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 9:
         if ((active0 & 0x2L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 10:
         if ((active0 & 0x2L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      case 11:
         if ((active0 & 0x2L) != 0L)
         {
            if (jjmatchedPos < 2)
            {
               jjmatchedKind = 2;
               jjmatchedPos = 2;
            }
            return -1;
         }
         return -1;
      default :
         return -1;
   }
}
private final int jjStartNfa_0(int pos, long active0)
{
   return jjMoveNfa_0(jjStopStringLiteralDfa_0(pos, active0), pos + 1);
}
private int jjStopAtPos(int pos, int kind)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}
private int jjMoveStringLiteralDfa0_0()
{
   switch(curChar)
   {
      case 101:
         return jjMoveStringLiteralDfa1_0(0x70L);
      case 123:
         return jjMoveStringLiteralDfa1_0(0x302L);
      default :
         return jjMoveNfa_0(0, 0);
   }
}
private int jjMoveStringLiteralDfa1_0(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(0, active0);
      return 1;
   }
   switch(curChar)
   {
      case 37:
         return jjMoveStringLiteralDfa2_0(active0, 0x302L);
      case 108:
         return jjMoveStringLiteralDfa2_0(active0, 0x50L);
      case 110:
         return jjMoveStringLiteralDfa2_0(active0, 0x20L);
      default :
         break;
   }
   return jjStartNfa_0(0, active0);
}
private int jjMoveStringLiteralDfa2_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(0, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(1, active0);
      return 2;
   }
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa3_0(active0, 0x302L);
      case 100:
         return jjMoveStringLiteralDfa3_0(active0, 0x20L);
      case 105:
         return jjMoveStringLiteralDfa3_0(active0, 0x10L);
      case 115:
         return jjMoveStringLiteralDfa3_0(active0, 0x40L);
      default :
         break;
   }
   return jjStartNfa_0(1, active0);
}
private int jjMoveStringLiteralDfa3_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(1, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(2, active0);
      return 3;
   }
   switch(curChar)
   {
      case 99:
         return jjMoveStringLiteralDfa4_0(active0, 0x2L);
      case 101:
         if ((active0 & 0x40L) != 0L)
            return jjStopAtPos(3, 6);
         return jjMoveStringLiteralDfa4_0(active0, 0x200L);
      case 102:
         if ((active0 & 0x10L) != 0L)
            return jjStopAtPos(3, 4);
         break;
      case 105:
         return jjMoveStringLiteralDfa4_0(active0, 0x120L);
      default :
         break;
   }
   return jjStartNfa_0(2, active0);
}
private int jjMoveStringLiteralDfa4_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(2, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(3, active0);
      return 4;
   }
   switch(curChar)
   {
      case 102:
         if ((active0 & 0x20L) != 0L)
            return jjStopAtPos(4, 5);
         return jjMoveStringLiteralDfa5_0(active0, 0x100L);
      case 108:
         return jjMoveStringLiteralDfa5_0(active0, 0x200L);
      case 111:
         return jjMoveStringLiteralDfa5_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(3, active0);
}
private int jjMoveStringLiteralDfa5_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(3, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(4, active0);
      return 5;
   }
   switch(curChar)
   {
      case 32:
         if ((active0 & 0x100L) != 0L)
            return jjStopAtPos(5, 8);
         break;
      case 105:
         return jjMoveStringLiteralDfa6_0(active0, 0x200L);
      case 109:
         return jjMoveStringLiteralDfa6_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(4, active0);
}
private int jjMoveStringLiteralDfa6_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(4, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(5, active0);
      return 6;
   }
   switch(curChar)
   {
      case 102:
         return jjMoveStringLiteralDfa7_0(active0, 0x200L);
      case 109:
         return jjMoveStringLiteralDfa7_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(5, active0);
}
private int jjMoveStringLiteralDfa7_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(5, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(6, active0);
      return 7;
   }
   switch(curChar)
   {
      case 32:
         if ((active0 & 0x200L) != 0L)
            return jjStopAtPos(7, 9);
         break;
      case 101:
         return jjMoveStringLiteralDfa8_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(6, active0);
}
private int jjMoveStringLiteralDfa8_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(6, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(7, active0);
      return 8;
   }
   switch(curChar)
   {
      case 110:
         return jjMoveStringLiteralDfa9_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(7, active0);
}
private int jjMoveStringLiteralDfa9_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(7, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(8, active0);
      return 9;
   }
   switch(curChar)
   {
      case 116:
         return jjMoveStringLiteralDfa10_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(8, active0);
}
private int jjMoveStringLiteralDfa10_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(8, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(9, active0);
      return 10;
   }
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa11_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(9, active0);
}
private int jjMoveStringLiteralDfa11_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(9, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(10, active0);
      return 11;
   }
   switch(curChar)
   {
      case 37:
         return jjMoveStringLiteralDfa12_0(active0, 0x2L);
      default :
         break;
   }
   return jjStartNfa_0(10, active0);
}
private int jjMoveStringLiteralDfa12_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(10, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(11, active0);
      return 12;
   }
   switch(curChar)
   {
      case 125:
         if ((active0 & 0x2L) != 0L)
            return jjStopAtPos(12, 1);
         break;
      default :
         break;
   }
   return jjStartNfa_0(11, active0);
}
private int jjMoveNfa_0(int startState, int curPos)
{
   int startsAt = 0;
   jjnewStateCnt = 8;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if (curChar == 37)
                     jjstateSet[jjnewStateCnt++] = 1;
                  else if (curChar == 32)
                     jjstateSet[jjnewStateCnt++] = 2;
                  break;
               case 4:
                  if (curChar != 37)
                     break;
                  if (kind > 2)
                     kind = 2;
                  jjstateSet[jjnewStateCnt++] = 5;
                  break;
               case 2:
                  if (curChar == 37)
                     jjstateSet[jjnewStateCnt++] = 1;
                  break;
               case 5:
                  if (curChar == 32 && kind > 2)
                     kind = 2;
                  break;
               case 7:
                  if (curChar == 32 && kind > 7)
                     kind = 7;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if (curChar == 123)
                     jjAddStates(0, 1);
                  break;
               case 4:
                  if (curChar != 123)
                     break;
                  if (kind > 7)
                     kind = 7;
                  jjstateSet[jjnewStateCnt++] = 7;
                  break;
               case 1:
                  if (curChar == 125)
                     kind = 3;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 8 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
private final int jjStopStringLiteralDfa_2(int pos, long active0)
{
   switch (pos)
   {
      default :
         return -1;
   }
}
private final int jjStartNfa_2(int pos, long active0)
{
   return jjMoveNfa_2(jjStopStringLiteralDfa_2(pos, active0), pos + 1);
}
private int jjMoveStringLiteralDfa0_2()
{
   switch(curChar)
   {
      case 124:
         return jjStopAtPos(0, 15);
      case 125:
         return jjMoveStringLiteralDfa1_2(0x10000L);
      default :
         return jjMoveNfa_2(0, 0);
   }
}
private int jjMoveStringLiteralDfa1_2(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_2(0, active0);
      return 1;
   }
   switch(curChar)
   {
      case 125:
         if ((active0 & 0x10000L) != 0L)
            return jjStopAtPos(1, 16);
         break;
      default :
         break;
   }
   return jjStartNfa_2(0, active0);
}
private int jjMoveNfa_2(int startState, int curPos)
{
   int startsAt = 0;
   jjnewStateCnt = 1;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if ((0x3ff400000000000L & l) == 0L)
                     break;
                  kind = 13;
                  jjstateSet[jjnewStateCnt++] = 0;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if ((0x7fffffe87fffffeL & l) == 0L)
                     break;
                  kind = 13;
                  jjstateSet[jjnewStateCnt++] = 0;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 1 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
private int jjMoveStringLiteralDfa0_3()
{
   return jjMoveNfa_3(1, 0);
}
private int jjMoveNfa_3(int startState, int curPos)
{
   int startsAt = 0;
   jjnewStateCnt = 4;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 1:
                  if ((0x3ff400000000000L & l) != 0L)
                  {
                     if (kind > 13)
                        kind = 13;
                     jjCheckNAdd(0);
                  }
                  else if (curChar == 37)
                     jjstateSet[jjnewStateCnt++] = 2;
                  else if (curChar == 32)
                     jjstateSet[jjnewStateCnt++] = 3;
                  break;
               case 0:
                  if ((0x3ff400000000000L & l) == 0L)
                     break;
                  kind = 13;
                  jjCheckNAdd(0);
                  break;
               case 3:
                  if (curChar == 37)
                     jjstateSet[jjnewStateCnt++] = 2;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 1:
               case 0:
                  if ((0x7fffffe87fffffeL & l) == 0L)
                     break;
                  kind = 13;
                  jjCheckNAdd(0);
                  break;
               case 2:
                  if (curChar == 125)
                     kind = 17;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 4 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
private int jjMoveStringLiteralDfa0_1()
{
   switch(curChar)
   {
      case 123:
         return jjMoveStringLiteralDfa1_1(0x800L);
      default :
         return 1;
   }
}
private int jjMoveStringLiteralDfa1_1(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 1;
   }
   switch(curChar)
   {
      case 37:
         return jjMoveStringLiteralDfa2_1(active0, 0x800L);
      default :
         return 2;
   }
}
private int jjMoveStringLiteralDfa2_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 2;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 2;
   }
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa3_1(active0, 0x800L);
      default :
         return 3;
   }
}
private int jjMoveStringLiteralDfa3_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 3;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 3;
   }
   switch(curChar)
   {
      case 101:
         return jjMoveStringLiteralDfa4_1(active0, 0x800L);
      default :
         return 4;
   }
}
private int jjMoveStringLiteralDfa4_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 4;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 4;
   }
   switch(curChar)
   {
      case 110:
         return jjMoveStringLiteralDfa5_1(active0, 0x800L);
      default :
         return 5;
   }
}
private int jjMoveStringLiteralDfa5_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 5;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 5;
   }
   switch(curChar)
   {
      case 100:
         return jjMoveStringLiteralDfa6_1(active0, 0x800L);
      default :
         return 6;
   }
}
private int jjMoveStringLiteralDfa6_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 6;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 6;
   }
   switch(curChar)
   {
      case 99:
         return jjMoveStringLiteralDfa7_1(active0, 0x800L);
      default :
         return 7;
   }
}
private int jjMoveStringLiteralDfa7_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 7;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 7;
   }
   switch(curChar)
   {
      case 111:
         return jjMoveStringLiteralDfa8_1(active0, 0x800L);
      default :
         return 8;
   }
}
private int jjMoveStringLiteralDfa8_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 8;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 8;
   }
   switch(curChar)
   {
      case 109:
         return jjMoveStringLiteralDfa9_1(active0, 0x800L);
      default :
         return 9;
   }
}
private int jjMoveStringLiteralDfa9_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 9;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 9;
   }
   switch(curChar)
   {
      case 109:
         return jjMoveStringLiteralDfa10_1(active0, 0x800L);
      default :
         return 10;
   }
}
private int jjMoveStringLiteralDfa10_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 10;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 10;
   }
   switch(curChar)
   {
      case 101:
         return jjMoveStringLiteralDfa11_1(active0, 0x800L);
      default :
         return 11;
   }
}
private int jjMoveStringLiteralDfa11_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 11;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 11;
   }
   switch(curChar)
   {
      case 110:
         return jjMoveStringLiteralDfa12_1(active0, 0x800L);
      default :
         return 12;
   }
}
private int jjMoveStringLiteralDfa12_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 12;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 12;
   }
   switch(curChar)
   {
      case 116:
         return jjMoveStringLiteralDfa13_1(active0, 0x800L);
      default :
         return 13;
   }
}
private int jjMoveStringLiteralDfa13_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 13;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 13;
   }
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa14_1(active0, 0x800L);
      default :
         return 14;
   }
}
private int jjMoveStringLiteralDfa14_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 14;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 14;
   }
   switch(curChar)
   {
      case 37:
         return jjMoveStringLiteralDfa15_1(active0, 0x800L);
      default :
         return 15;
   }
}
private int jjMoveStringLiteralDfa15_1(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return 15;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      return 15;
   }
   switch(curChar)
   {
      case 125:
         if ((active0 & 0x800L) != 0L)
            return jjStopAtPos(15, 11);
         break;
      default :
         return 16;
   }
   return 16;
}
static final int[] jjnextStates = {
   4, 6, 
};

/** Token literal values. */
public static final String[] jjstrLiteralImages = {
"", null, null, null, "\145\154\151\146", "\145\156\144\151\146", 
"\145\154\163\145", null, "\173\45\40\151\146\40", "\173\45\40\145\154\151\146\40", null, null, 
null, null, null, "\174", "\175\175", null, };

/** Lexer state names. */
public static final String[] lexStateNames = {
   "DEFAULT",
   "IN_COMMENT",
   "IN_VAR",
   "IN_CONDITION",
};

/** Lex State array. */
public static final int[] jjnewLexState = {
   -1, 1, -1, -1, -1, -1, -1, 2, 3, 3, -1, 0, -1, -1, -1, -1, 0, 0, 
};
static final long[] jjtoToken = {
   0x3a7fdL, 
};
static final long[] jjtoSkip = {
   0x4802L, 
};
static final long[] jjtoMore = {
   0x1000L, 
};
protected SimpleCharStream input_stream;
private final int[] jjrounds = new int[8];
private final int[] jjstateSet = new int[16];
protected char curChar;
/** Constructor. */
public TemplateParserTokenManager(SimpleCharStream stream){
   if (SimpleCharStream.staticFlag)
      throw new Error("ERROR: Cannot use a static CharStream class with a non-static lexical analyzer.");
   input_stream = stream;
}

/** Constructor. */
public TemplateParserTokenManager(SimpleCharStream stream, int lexState){
   this(stream);
   SwitchTo(lexState);
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream)
{
   jjmatchedPos = jjnewStateCnt = 0;
   curLexState = defaultLexState;
   input_stream = stream;
   ReInitRounds();
}
private void ReInitRounds()
{
   int i;
   jjround = 0x80000001;
   for (i = 8; i-- > 0;)
      jjrounds[i] = 0x80000000;
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream, int lexState)
{
   ReInit(stream);
   SwitchTo(lexState);
}

/** Switch to specified lex state. */
public void SwitchTo(int lexState)
{
   if (lexState >= 4 || lexState < 0)
      throw new TokenMgrError("Error: Ignoring invalid lexical state : " + lexState + ". State unchanged.", TokenMgrError.INVALID_LEXICAL_STATE);
   else
      curLexState = lexState;
}

protected Token jjFillToken()
{
   final Token t;
   final String curTokenImage;
   final int beginLine;
   final int endLine;
   final int beginColumn;
   final int endColumn;
   String im = jjstrLiteralImages[jjmatchedKind];
   curTokenImage = (im == null) ? input_stream.GetImage() : im;
   beginLine = input_stream.getBeginLine();
   beginColumn = input_stream.getBeginColumn();
   endLine = input_stream.getEndLine();
   endColumn = input_stream.getEndColumn();
   t = Token.newToken(jjmatchedKind, curTokenImage);

   t.beginLine = beginLine;
   t.endLine = endLine;
   t.beginColumn = beginColumn;
   t.endColumn = endColumn;

   return t;
}

int curLexState = 0;
int defaultLexState = 0;
int jjnewStateCnt;
int jjround;
int jjmatchedPos;
int jjmatchedKind;

/** Get the next Token. */
public Token getNextToken() 
{
  Token matchedToken;
  int curPos = 0;

  EOFLoop :
  for (;;)
  {
   try
   {
      curChar = input_stream.BeginToken();
   }
   catch(java.io.IOException e)
   {
      jjmatchedKind = 0;
      matchedToken = jjFillToken();
      return matchedToken;
   }

   for (;;)
   {
     switch(curLexState)
     {
       case 0:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_0();
         if (jjmatchedPos == 0 && jjmatchedKind > 10)
         {
            jjmatchedKind = 10;
         }
         break;
       case 1:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_1();
         if (jjmatchedPos == 0 && jjmatchedKind > 12)
         {
            jjmatchedKind = 12;
         }
         break;
       case 2:
         try { input_stream.backup(0);
            while (curChar <= 32 && (0x100000000L & (1L << curChar)) != 0L)
               curChar = input_stream.BeginToken();
         }
         catch (java.io.IOException e1) { continue EOFLoop; }
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_2();
         break;
       case 3:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_3();
         break;
     }
     if (jjmatchedKind != 0x7fffffff)
     {
        if (jjmatchedPos + 1 < curPos)
           input_stream.backup(curPos - jjmatchedPos - 1);
        if ((jjtoToken[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
        {
           matchedToken = jjFillToken();
       if (jjnewLexState[jjmatchedKind] != -1)
         curLexState = jjnewLexState[jjmatchedKind];
           return matchedToken;
        }
        else if ((jjtoSkip[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
        {
         if (jjnewLexState[jjmatchedKind] != -1)
           curLexState = jjnewLexState[jjmatchedKind];
           continue EOFLoop;
        }
      if (jjnewLexState[jjmatchedKind] != -1)
        curLexState = jjnewLexState[jjmatchedKind];
        curPos = 0;
        jjmatchedKind = 0x7fffffff;
        try {
           curChar = input_stream.readChar();
           continue;
        }
        catch (java.io.IOException e1) { }
     }
     int error_line = input_stream.getEndLine();
     int error_column = input_stream.getEndColumn();
     String error_after = null;
     boolean EOFSeen = false;
     try { input_stream.readChar(); input_stream.backup(1); }
     catch (java.io.IOException e1) {
        EOFSeen = true;
        error_after = curPos <= 1 ? "" : input_stream.GetImage();
        if (curChar == '\n' || curChar == '\r') {
           error_line++;
           error_column = 0;
        }
        else
           error_column++;
     }
     if (!EOFSeen) {
        input_stream.backup(1);
        error_after = curPos <= 1 ? "" : input_stream.GetImage();
     }
     throw new TokenMgrError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, TokenMgrError.LEXICAL_ERROR);
   }
  }
}

private void jjCheckNAdd(int state)
{
   if (jjrounds[state] != jjround)
   {
      jjstateSet[jjnewStateCnt++] = state;
      jjrounds[state] = jjround;
   }
}
private void jjAddStates(int start, int end)
{
   do {
      jjstateSet[jjnewStateCnt++] = jjnextStates[start];
   } while (start++ != end);
}
private void jjCheckNAddTwoStates(int state1, int state2)
{
   jjCheckNAdd(state1);
   jjCheckNAdd(state2);
}

}
